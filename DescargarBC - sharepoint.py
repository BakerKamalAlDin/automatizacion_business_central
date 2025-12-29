# -*- coding: utf-8 -*-
import os
import unicodedata
import time
import shutil
import urllib.parse
import csv
import sys
import re
import threading
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Intento de importar calamine para asegurar que pandas lo detecte si es necesario
try:
    import python_calamine
except ImportError:
    pass

# --- CONFIGURACIÓN DE RUTAS ---
ruta_usuario_txt = r"C:\ficheros python\usuarioContraseña.txt"
ruta_usuario_sp_txt = r"C:\ficheros python\usuarioContraseñaSharePoint.txt" # Para SharePoint
ruta_descarga_txt = r"C:\ficheros python\carpetaChromeDescargas.txt"
ruta_enlaces_txt = r"C:\ficheros python\enlaces.txt"
ruta_empresas_txt = r"C:\ficheros python\Empresas.txt"
ruta_csv_proyectos = r"C:\ficheros python\Proyecto a borrar.csv"
ruta_dp_responsable = r"C:\ficheros python\DP_RESPONSABLE.xlsx" 
ruta_log = "log_proceso.txt"

# Rutas Base
ruta_base_bc = r"C:\ArchivosBC"
ruta_excel_base = os.path.join(ruta_base_bc, "Excel")
ruta_csv_base = os.path.join(ruta_base_bc, "CSV")
ruta_errores = os.path.join(ruta_base_bc, "Errores")
dir_base_hilos = r"C:\ficheros python\Temp_Workers"

# --- DEFINICIÓN DE ESTRUCTURA FINAL (41 COLUMNAS) ---
COLUMNAS_FINALES = [
    "EMPRESA", "DP", "RESPONSABLE", "Fecha registro", "Descripción", "Nº proyecto", "Tipo movimiento", 
    "Nº tarea proyecto", "Nº Pedido Cliente", "Nº acta Cliente", "PRECIO UNIDAD", "Cantidad producción actual", 
    "PRODUCCIÓN", "FACTURACIÓN", "O.C", "Ejercicio", "Nº documento", "Fecha emisión documento", "Nº Cliente", 
    "Nombre cliente", "Nº documento externo", "Nº preasignado", "Nº documento relacionado cruzada", 
    "Cód. empresa relacionada cruzadas", "Nº documento original cruzadas", "Fecha original cruzadas", 
    "Existe producción", "Existe certificación", "Destino Final", "Tipo", "Cuenta", "Tipo mov. cont.", 
    "Nº mov.", "Id. usuario", "COD. FAMILIA RECURSO", "Grado de avance", "Cantidad producción total", 
    "Código", "Comentarios", "Proyecto Cerrado", "Grupo registro IVA prod"
]

log_lock = threading.Lock()
df_responsables_global = None

def limpiar_nombre_archivo(nombre):
    limpio = re.sub(r'[\\/*?:"<>|]', "", nombre)
    return limpio.strip()

def inicializar_entorno():
    """Crea estructura, limpia temporales y carga tablas auxiliares."""
    global df_responsables_global
    
    for carpeta in [ruta_base_bc, ruta_excel_base, ruta_csv_base, ruta_errores]:
        os.makedirs(carpeta, exist_ok=True)
    
    if os.path.exists(dir_base_hilos):
        shutil.rmtree(dir_base_hilos, ignore_errors=True)
    os.makedirs(dir_base_hilos, exist_ok=True)

    escribir_log("Cargando tabla maestra de Responsables...")
    try:
        if os.path.exists(ruta_dp_responsable):
            # Usamos calamine aquí también para carga ultrarrápida
            try:
                df_resp = pd.read_excel(ruta_dp_responsable, engine="calamine")
            except:
                df_resp = pd.read_excel(ruta_dp_responsable, engine="openpyxl")

            df_resp = df_resp.rename(columns={"COD. DP": "DP_KEY", "NOMBRE ENCARGADO": "RESPONSABLE_LOOKUP"})
            df_resp["DP_KEY"] = df_resp["DP_KEY"].astype(str).str.strip()
            df_responsables_global = df_resp[["DP_KEY", "RESPONSABLE_LOOKUP"]]
            escribir_log(f"Tabla Responsables cargada: {len(df_responsables_global)} filas.")
        else:
            escribir_log("ADVERTENCIA: No se encontró archivo DP_RESPONSABLE.", consola=True)
            df_responsables_global = pd.DataFrame(columns=["DP_KEY", "RESPONSABLE_LOOKUP"])
    except Exception as e:
        escribir_log(f"ERROR cargando Responsables: {e}", consola=True)
        df_responsables_global = pd.DataFrame(columns=["DP_KEY", "RESPONSABLE_LOOKUP"])

def escribir_log(mensaje, consola=False):
    linea = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}"
    with log_lock:
        try:
            with open(ruta_log, "a", encoding="utf-8") as log:
                log.write(linea + "\n")
        except: pass
    if consola: print(linea)

def esperar_pagina_cargada(driver, timeout=40):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        return True
    except: return False

def archivo_estable(ruta, intentos=5, espera=2):
    if ruta.endswith('.crdownload') or ruta.endswith('.tmp'): return False
    tam_anterior = -1
    for _ in range(intentos):
        if not os.path.exists(ruta):
            time.sleep(espera)
            continue
        try:
            tam_actual = os.path.getsize(ruta)
            if tam_actual > 0 and tam_actual == tam_anterior:
                with open(ruta, "rb"): return True
            tam_anterior = tam_actual
        except: pass
        time.sleep(espera)
    return False

def realizar_login(driver, wait, usuario, password):
    for _ in range(3):
        try:
            driver.get("https://bc.zener.es/ZENER_BC/")
            if not esperar_pagina_cargada(driver): continue
            
            user_field = wait.until(EC.element_to_be_clickable((By.ID, "UserName")))
            user_field.clear()
            user_field.send_keys(usuario)
            
            pass_field = driver.find_element(By.ID, "Password")
            pass_field.clear()
            pass_field.send_keys(password)
            
            driver.find_element(By.ID, "submitButton").click()
            wait.until(EC.url_contains("ZENER_BC"))
            return True
        except:
            time.sleep(3)
            driver.delete_all_cookies()
            driver.refresh()
    return False

def transformar_datos_powerquery(df, categoria, empresa):
    try:
        df.columns = df.columns.str.strip()
        df["EMPRESA"] = empresa
        es_certificacion = "certificac" in categoria.lower()

        # --- LIMPIADOR NUMÉRICO SEGURO (ES → PYTHON, una sola vez) ---
        def safe_num(series):
            if series is None:
                return 0

            s = series.astype(str).str.strip()

            def corregir_formato(val):
                if not val or val.lower() == "nan":
                    return "0"
                
                val = val.replace("\u00a0", "").replace(" ", "")

                # Si hay coma, asumimos formato europeo 1.234,56
                if "," in val:
                    return val.replace(".", "").replace(",", ".")
                # Si no hay coma, ya está en formato Python
                return val

            s = s.apply(corregir_formato)
            return pd.to_numeric(s, errors="coerce").fillna(0)

        # --- MAPEO DE COLUMNAS ---
        renames = {
            "COD. DP": "DP",
            "Nº Documento": "Nº documento",
            "No. documento": "Nº documento",
            "Nº proveedor/cliente": "Nº Cliente",
            "Nombre proveedor/cliente": "Nombre cliente",
            "Existe Certificacón": "Existe certificación",
            "Fecha Registro": "Fecha registro",
            "Nº Acta Cliente": "Nº acta Cliente",
        }

        if not es_certificacion:
            renames.update({
                "Cantidad": "PRECIO UNIDAD",
                "Precio venta (DL)": "Cantidad producción actual"
            })
        else:
            renames.update({"Nº": "Cuenta"})

        df = df.rename(columns=renames)

        # --- CÁLCULOS ---
        if not es_certificacion:
            df["PRODUCCIÓN"] = 0.0

            col_imp = "Importe línea (DL)" if "Importe línea (DL)" in df.columns else "Importe"
            val_fact = safe_num(df[col_imp])

            df["FACTURACIÓN"] = -val_fact
            df["O.C"] = df["PRODUCCIÓN"] - df["FACTURACIÓN"]

            df["PRECIO UNIDAD"] = safe_num(df["PRECIO UNIDAD"])
            df["Cantidad producción actual"] = safe_num(df["Cantidad producción actual"])

        else:
            val_prod = safe_num(df["Importe producción actual venta (DL)"])
            cant_act = safe_num(df["Cantidad producción actual"])

            df["PRODUCCIÓN"] = val_prod
            df["FACTURACIÓN"] = 0.0
            df["O.C"] = df["PRODUCCIÓN"] - df["FACTURACIÓN"]
            df["Cantidad producción actual"] = cant_act

            df["PRECIO UNIDAD"] = np.where(
                cant_act != 0,
                (val_prod / cant_act).round(4),
                0
            )

            df["Tipo movimiento"] = "Producción"

        # --- CRUCE RESPONSABLE ---
        if "DP" in df.columns and df_responsables_global is not None:
            df["DP_TEMP"] = df["DP"].astype(str).str.strip()
            df = df.merge(
                df_responsables_global,
                left_on="DP_TEMP",
                right_on="DP_KEY",
                how="left"
            )
            df["RESPONSABLE"] = df["RESPONSABLE_LOOKUP"]
            df = df.drop(
                columns=["DP_TEMP", "DP_KEY", "RESPONSABLE_LOOKUP"],
                errors="ignore"
            )

        # --- ESTRUCTURA FINAL ---
        for col in COLUMNAS_FINALES:
            if col not in df.columns:
                df[col] = None

        return df[COLUMNAS_FINALES].copy()

    except Exception as e:
        escribir_log(f"Error en transformación: {e}")
        return None

def procesar_descarga(id_hilo, tarea, empresa, usuario, password, filtros_proyectos, max_intentos=3):
    categoria_raw = tarea['prefijo']
    categoria_clean = limpiar_nombre_archivo(categoria_raw)
    
    dir_destino_excel = os.path.join(ruta_excel_base, categoria_clean)
    dir_destino_csv = os.path.join(ruta_csv_base, categoria_clean)
    
    os.makedirs(dir_destino_excel, exist_ok=True)
    os.makedirs(dir_destino_csv, exist_ok=True)

    nombre_tarea = f"[{empresa} - {categoria_raw}]"
    clean_emp = limpiar_nombre_archivo(empresa).replace(' ', '_')
    time.sleep(((id_hilo - 1) % 3) * 5)

    for intento in range(1, max_intentos + 1):
        dir_hilo = os.path.join(dir_base_hilos, f"worker_{id_hilo}_{intento}")
        os.makedirs(dir_hilo, exist_ok=True)
        escribir_log(f"Iniciando {nombre_tarea} (Intento {intento})", consola=True)

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": dir_hilo,
            "download.prompt_for_download": False,
            "safebrowsing.enabled": True
        })

        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": dir_hilo})
        wait = WebDriverWait(driver, 90)

        try:
            if not realizar_login(driver, wait, usuario, password): raise Exception("Login fallido")
            
            filtro_csv = filtros_proyectos.get(empresa, "%26%3c%3e".join(["*ES000*", "*MODES*", "*CRU*", "*MARGEN*"]))
            empresa_encoded = urllib.parse.quote(empresa.strip(), safe="", encoding="utf-8")
            url_final = tarea['url'].strip().replace("empresas.txt", empresa_encoded).replace("Proyecto a borrar.csv", filtro_csv)
            
            driver.get(url_final)
            wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, "/html/body/div[2]/div[2]/div[1]/div/div[1]/div/iframe")))

            xpath_menu = "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button"
            xpath_descarga = "/html/body/div[1]/div[2]/form/div/div[2]/div[5]/div/div/div/div[3]/div/div/ul/li/button"
            
            btn_menu = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_menu)))
            driver.execute_script("arguments[0].click();", btn_menu)
            time.sleep(2)
            btn_desc = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_descarga)))
            driver.execute_script("arguments[0].click();", btn_desc)

            inicio = time.time()
            archivo = None
            while time.time() - inicio < 3600:
                files = [f for f in os.listdir(dir_hilo) if not f.endswith(('.crdownload', '.tmp'))]
                if files:
                    pos = os.path.join(dir_hilo, files[0])
                    if os.path.getsize(pos) > 500 and archivo_estable(pos):
                        archivo = pos
                        break
                time.sleep(5)

            if not archivo: raise Exception("Timeout Descarga")

            nuevo_nombre = f"{clean_emp}_{categoria_clean}_{datetime.now().strftime('%H%M%S')}"
            ruta_final_excel = os.path.join(dir_destino_excel, nuevo_nombre + ".xlsx")
            shutil.move(archivo, ruta_final_excel)

            # --- LÓGICA DE LECTURA OPTIMIZADA CON CALAMINE ---
            try:
                # 1. Intentamos con Calamine (Rust) = MUY RÁPIDO
                df_raw = pd.read_excel(ruta_final_excel, engine="calamine")
            except Exception as e_calamine:
                # 2. Si falla (por versión de pandas o formato raro), usamos openpyxl
                escribir_log(f"WARN: Calamine falló en {nombre_tarea}, usando openpyxl. Error: {e_calamine}")
                df_raw = pd.read_excel(ruta_final_excel, engine="openpyxl")
            
            if len(df_raw) == 0:
                escribir_log(f"AVISO: {nombre_tarea} vacío. No se genera CSV.")
                return {"status": "OK", "categoria": categoria_clean, "msg": "Vacio"}

            df_transformado = transformar_datos_powerquery(df_raw, categoria_raw, empresa)
            
            if df_transformado is not None:
                ruta_final_csv = os.path.join(dir_destino_csv, nuevo_nombre + ".csv")
                df_transformado.to_csv(ruta_final_csv, sep=";", index=False, encoding="utf-8-sig")
                escribir_log(f"OK {nombre_tarea} - {len(df_transformado)} filas.")
            else:
                raise Exception("Fallo transformación")

            return {"status": "OK", "categoria": categoria_clean}

        except Exception as e:
            escribir_log(f"ERROR {nombre_tarea}: {e}")
            try: driver.save_screenshot(os.path.join(ruta_errores, f"ERR_{clean_emp}_{intento}.png"))
            except: pass
            time.sleep(5)
        finally:
            driver.quit()
            shutil.rmtree(dir_hilo, ignore_errors=True)

    return {"status": "ERROR", "empresa": empresa, "tarea": tarea}

def limpiar_directorio_recursivo(directorio_base):
    if os.path.exists(directorio_base):
        shutil.rmtree(directorio_base)
    os.makedirs(directorio_base)

def limpiar_columnas_maestro(df):
    """
    Limpieza segura del DataFrame antes del CSV final.

    PRINCIPIOS:
    - Las columnas numéricas NO se transforman (evita doble normalización).
    - Las columnas de texto se limpian de separadores peligrosos.
    - No se alteran decimales, signos ni magnitudes.
    """

    # Columnas que representan importes o cantidades
    COLUMNAS_NUMERICAS = {
        "PRECIO UNIDAD",
        "Cantidad producción actual",
        "PRODUCCIÓN",
        "FACTURACIÓN",
        "O.C",
        "Cantidad producción total",
        "Grado de avance"
    }

    # 1. Rellenar NaN con vacío (necesario para operaciones string)
    df = df.fillna("")

    # 2. Limpieza controlada columna a columna
    for col in df.columns:
        if col in COLUMNAS_NUMERICAS:
            # NO tocar números ya normalizados
            df[col] = df[col].astype(str).str.strip()
        else:
            df[col] = (
                df[col].astype(str)
                      .str.replace(';', ',', regex=False)
                      .str.replace('"', "'", regex=False)
                      .str.replace(r'[\n\r]+', ' ', regex=True)
                      .str.strip()
                      .str.replace(r'\s{2,}', ' ', regex=True)
            )

    # 3. Eliminar filas completamente vacías
    df = df[df.astype(str).ne("").any(axis=1)]

    return df

def consolidar_archivos_por_categoria():
    print("\n" + "-"*40)
    print("[*] Iniciando Consolidación Estricta con Limpieza...")
    try:
        subcarpetas = [d for d in os.listdir(ruta_csv_base) if os.path.isdir(os.path.join(ruta_csv_base, d))]
    except FileNotFoundError: 
        return

    total_consolidados = 0

    for carpeta_cat in subcarpetas:
        ruta_cat_csv = os.path.join(ruta_csv_base, carpeta_cat)
        archivos_csv = glob.glob(os.path.join(ruta_cat_csv, "*.csv"))
        
        if archivos_csv:
            print(f"    > Procesando '{carpeta_cat}': {len(archivos_csv)} archivos.")
            try:
                df_list = []
                for f in archivos_csv:
                    # Leemos con motor python para mayor flexibilidad con carácteres raros
                    temp_df = pd.read_csv(f, sep=";", encoding="utf-8-sig", dtype=str, engine='python', on_bad_lines='warn')
                    
                    # Aseguramos columnas antes de limpiar
                    for col in COLUMNAS_FINALES:
                        if col not in temp_df.columns:
                            temp_df[col] = "" # Directamente vacío para evitar NaN
                    
                    temp_df = temp_df[COLUMNAS_FINALES]
                    df_list.append(temp_df)

                # Unimos todos los archivos
                df_consolidado = pd.concat(df_list, ignore_index=True, sort=False)
                
                # --- LIMPIEZA REFORZADA: Elimina saltos de línea y repara separadores ---
                print(f"      [Limpiando] Normalizando estructura en '{carpeta_cat}'...")
                df_consolidado = limpiar_columnas_maestro(df_consolidado)
                
                # Guardado final
                nombre_final = f"{carpeta_cat} Unidos.csv"
                ruta_salida = os.path.join(ruta_base_bc, nombre_final)
                
                # Usamos quoting=csv.QUOTE_MINIMAL para asegurar que si hay comas internas no rompa nada
                df_consolidado.to_csv(ruta_salida, index=False, sep=";", encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
                
                print(f"      [OK] Generado: {nombre_final} ({len(df_consolidado)} filas)")
                total_consolidados += 1
            except Exception as e:
                print(f"      [ERROR] Fallo en '{carpeta_cat}': {e}")
        else:
            print(f"    > Omitiendo '{carpeta_cat}': Vacía.")
    print(f"[*] Consolidación finalizada. {total_consolidados} maestros creados.")

# --- NUEVA FUNCIÓN PARA SHAREPOINT (Añadir con las demás funciones) ---
def subir_a_sharepoint_zener(ruta_local_archivo):
    """
    Sube archivos grandes a SharePoint mediante fragmentos (chunks).
    Usa credenciales independientes desde usuarioContraseñaSharePoint.txt
    """
    try:
        from office365.sharepoint.client_context import ClientContext
        from office365.runtime.auth.user_credential import UserCredential
    except ImportError:
        escribir_log("ERROR: Falta la librería 'Office365-REST-Python-Client'.", consola=True)
        return

    site_url = "https://zenerorg.sharepoint.com/sites/g365_facturacion"
    
    try:
        # --- CAMBIO AQUI: Usamos el fichero específico de SharePoint ---
        if not os.path.exists(ruta_usuario_sp_txt):
            raise Exception(f"No existe el archivo de credenciales: {ruta_usuario_sp_txt}")

        with open(ruta_usuario_sp_txt, "r", encoding="utf-8") as f:
            datos = [l.strip() for l in f.readlines() if l.strip()]
            if len(datos) < 2:
                raise Exception("El archivo usuarioContraseñaSharePoint.txt debe tener al menos 2 líneas (Usuario y Password).")
            user_email = datos[0]
            password = datos[1]
        # ---------------------------------------------------------------

        ctx = ClientContext(site_url).with_credentials(UserCredential(user_email, password))
        
        # 2. Localizar carpeta
        target_folder_url = "/sites/g365_facturacion/Shared Documents/Projects/datos"
        
        try:
            target_folder = ctx.web.get_folder_by_server_relative_url(target_folder_url)
            ctx.load(target_folder)
            ctx.execute_query()
        except Exception:
            target_folder_url_alt = "/sites/g365_facturacion/Projects/datos"
            try:
                target_folder = ctx.web.get_folder_by_server_relative_url(target_folder_url_alt)
                ctx.load(target_folder)
                ctx.execute_query()
            except:
                raise Exception(f"No se encuentra la carpeta destino en SharePoint.")

        # 3. Subida por fragmentos
        size = os.path.getsize(ruta_local_archivo)
        file_name = os.path.basename(ruta_local_archivo)
        print(f"[*] Subiendo a SharePoint: {file_name} ({round(size/1024/1024, 2)} MB)...")

        def print_progress(offset):
            print(f"   -> Subido: {round(offset/1024/1024, 2)} MB")

        chunk_size = 10 * 1024 * 1024 
        
        target_folder.files.create_upload_session(ruta_local_archivo, chunk_size, print_progress).execute_query()
            
        escribir_log(f"SHAREPOINT OK: {file_name} subido correctamente.", consola=True)

    except Exception as e:
        error_msg = f"ERROR SHAREPOINT en {os.path.basename(ruta_local_archivo)}: {str(e)}"
        escribir_log(error_msg, consola=True)
        print(f"[!] {error_msg}")

# --- BLOQUE MAIN MODIFICADO ---
if __name__ == "__main__":
    inicializar_entorno()
    inicio_global = datetime.now()
    print(f"=== INICIO PROCESO: {inicio_global.strftime('%H:%M:%S')} ===")

    try:
        # Carga de archivos de configuración
        with open(ruta_usuario_txt, "r", encoding="utf-8") as f:
            datos_login = [l.strip() for l in f.readlines() if l.strip()]
        
        empresas = [l.strip() for l in open(ruta_empresas_txt, "r", encoding="utf-8") if l.strip()]
        enlaces_raw = [l.strip() for l in open(ruta_enlaces_txt, "r", encoding="utf-8") if l.strip()]

        # Limpieza de carpetas de trabajo local
        limpiar_directorio_recursivo(ruta_excel_base)
        limpiar_directorio_recursivo(ruta_csv_base)

        # Lógica de filtros de proyectos
        filtros_proyectos = {}
        if os.path.exists(ruta_csv_proyectos):
            try:
                df_filtros = pd.read_csv(ruta_csv_proyectos, sep=None, engine='python', encoding='utf-8-sig')
                for _, row in df_filtros.iterrows():
                    emp = str(row["EMPRESA"]).strip()
                    proy = str(row["PROYECTOS A ELIMINAR"]).strip()
                    if emp and proy: filtros_proyectos.setdefault(emp, []).append(proy)
            except: pass
        
        sep_url = "%26%3c%3e"
        fijos = ["*ES000*", "*MODES*", "*CRU*", "*MARGEN*"]
        for emp in filtros_proyectos:
            filtros_proyectos[emp] = sep_url.join(filtros_proyectos[emp] + fijos)

        # Preparación de tareas
        tareas_base = []
        for i in range(0, len(enlaces_raw)-1, 2):
            tareas_base.append({"url": enlaces_raw[i], "prefijo": enlaces_raw[i+1]})

        trabajos = []
        id_gen = 1
        for emp in sorted(set(empresas)):
            for tarea in tareas_base:
                trabajos.append((id_gen, tarea, emp, datos_login[0], datos_login[1], filtros_proyectos))
                id_gen += 1

        # Ejecución de descargas en rondas (Máximo 3 reintentos)
        pendientes = trabajos
        ronda = 1
        while pendientes and ronda <= 3:
            print(f"\n[*] RONDA {ronda}: Procesando {len(pendientes)} trabajos...")
            with ThreadPoolExecutor(max_workers=3) as executor:
                resultados = list(executor.map(lambda p: procesar_descarga(*p), pendientes))

            nuevos_pendientes = []
            for i, r in enumerate(resultados):
                if r.get("status") != "OK":
                    job = list(pendientes[i])
                    job[0] += (ronda * 1000)
                    nuevos_pendientes.append(tuple(job))
            
            pendientes = nuevos_pendientes
            ronda += 1

        # 1. Consolidar archivos localmente
        consolidar_archivos_por_categoria()

        # 2. SUBIDA A SHAREPOINT (Nuevo paso final)
        print("\n" + "="*40)
        print("[*] Iniciando Fase de Subida a SharePoint...")
        archivos_maestros = glob.glob(os.path.join(ruta_base_bc, "* Unidos.csv"))
        
        if archivos_maestros:
            for archivo in archivos_maestros:
                subir_a_sharepoint_zener(archivo)
        else:
            print("[!] No se encontraron archivos 'Unidos.csv' para subir.")

        print(f"\n=== FIN === Tiempo total: {datetime.now() - inicio_global}")

    except Exception as e:
        print(f"[FATAL] {e}")
        escribir_log(f"FATAL MAIN: {e}")
