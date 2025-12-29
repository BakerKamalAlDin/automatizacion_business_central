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
ruta_csv_project = os.path.join(ruta_base_bc, "csvProject") # <--- NUEVA RUTA
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
    
    # Añadida ruta_csv_project a la creación de carpetas iniciales (por seguridad)
    for carpeta in [ruta_base_bc, ruta_excel_base, ruta_csv_base, ruta_errores, ruta_csv_project]:
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
        # 1. Normalización de cabeceras
        df.columns = df.columns.str.strip()
        df["EMPRESA"] = empresa
        es_certificacion = "certificac" in categoria.lower()

        # --- LIMPIADOR NUMÉRICO ---
        def safe_num(series):
            if series is None: return 0
            s = series.astype(str).str.strip().replace(r'[\u00a0 ]', '', regex=True)
            s = s.apply(lambda x: x.replace('.', '').replace(',', '.') if ',' in x else x)
            return pd.to_numeric(s, errors="coerce").fillna(0)

        # --- DICCIONARIO DE RENOMBRADO (Basado en tus columnas reales) ---
        if not es_certificacion:
            # RAMA A: MOVS PROYECTOS
            renames = {
                "COD. DP": "DP",
                "Fecha registro": "Fecha registro", # Ya coincide, pero aseguramos
                "Nº documento": "Nº documento",
                "Cantidad": "PRECIO UNIDAD", 
                "Precio venta (DL)": "Cantidad producción actual",
                "Nº proveedor/cliente": "Nº Cliente",
                "Nombre proveedor/cliente": "Nombre cliente",
                "Existe Certificacón": "Existe certificación",
                "Nº": "Cuenta" # En Movs también existe según tu lista
            }
        else:
            # RAMA B: CERTIFICACIONES
            renames = {
                "COD. DP": "DP",
                "Fecha Registro": "Fecha registro",
                "Nº Acta Cliente": "Nº acta Cliente",
                "Nº": "Cuenta", # <--- ESTO SOLUCIONA TU DUDA
                "Nombre cliente": "Nombre cliente",
                "Nº Cliente": "Nº Cliente"
            }

        df = df.rename(columns=renames)

        # --- LÓGICA DE CÁLCULOS (Power Query Match) ---
        if not es_certificacion:
            df["PRODUCCIÓN"] = 0.0
            # Usar 'Importe línea (DL)' para facturación
            val_fact = safe_num(df["Importe línea (DL)"]) if "Importe línea (DL)" in df.columns else 0
            df["FACTURACIÓN"] = -val_fact
            
            # Ejercicio
            if "Fecha emisión documento" in df.columns:
                fechas = pd.to_datetime(df["Fecha emisión documento"], dayfirst=True, errors='coerce')
                df["Ejercicio"] = fechas.dt.year
        else:
            # Cálculos de Certificación
            val_prod = safe_num(df["Importe producción actual venta (DL)"])
            cant_act = safe_num(df["Cantidad producción actual"])
            
            df["PRODUCCIÓN"] = val_prod
            df["FACTURACIÓN"] = 0.0
            df["PRECIO UNIDAD"] = np.where(cant_act != 0, (val_prod / cant_act).round(4), 0)
            df["Tipo movimiento"] = "Producción"

        # O.C común para ambos
        df["O.C"] = df.get("PRODUCCIÓN", 0) - df.get("FACTURACIÓN", 0)

        # --- CRUCE CON RESPONSABLE ---
        if "DP" in df.columns and df_responsables_global is not None:
            df["DP"] = df["DP"].astype(str).str.strip()
            df = df.merge(df_responsables_global, left_on="DP", right_on="DP_KEY", how="left")
            df["RESPONSABLE"] = df["RESPONSABLE_LOOKUP"]

        # --- APLICAR EL MOLDE (Table.SelectColumns de Power Query) ---
        # Si la columna no existe en el origen, se crea con vacíos
        for col in COLUMNAS_FINALES:
            if col not in df.columns:
                df[col] = np.nan

        return df[COLUMNAS_FINALES].copy()

    except Exception as e:
        escribir_log(f"Error en transformación {categoria}: {e}")
        return None

# descargas

def configurar_driver(dir_hilo):
    """Configura las opciones de Chrome y el comportamiento de descarga."""
    chrome_options = Options()
    #chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": dir_hilo,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    })
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": dir_hilo})
    return driver

def navegar_y_preparar_descarga(driver, wait, url_final):
    """Gestiona la navegación, el iframe y la apertura del menú de Excel."""
    driver.get(url_final)
    
    # 1. Esperar Iframe
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, "/html/body/div[2]/div[2]/div[1]/div/div[1]/div/iframe")))
    
    # 2. Localizar botón Menú (XPath dinámico de tus instrucciones)
    xpath_menu = "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button"
    
    try:
        btn_menu = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_menu)))
    except:
        # Fallback si el XPath dinámico falla
        btn_menu = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@title, 'Abrir en Excel')]")))
    
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn_menu)
    time.sleep(1)
    driver.execute_script("arguments[0].click();", btn_menu)
    
    # 3. Botón final de descarga
    xpath_descarga = "/html/body/div[1]/div[2]/form/div/div[2]/div[5]/div/div/div/div[3]/div/div/ul/li/button"
    btn_desc = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_descarga)))
    driver.execute_script("arguments[0].click();", btn_desc)
    time.sleep(2)

def procesar_empresa_completa(id_hilo, empresa, usuario, password, tareas_base, filtros_proyectos):
    """
    Versión mejorada con aviso de ficheros vacíos.
    """
    inicio_empresa = datetime.now()
    clean_emp = limpiar_nombre_archivo(empresa).replace(' ', '_')
    dir_hilo = os.path.join(dir_base_hilos, f"worker_emp_{id_hilo}")
    os.makedirs(dir_hilo, exist_ok=True)

    escribir_log(f"[HILO {id_hilo}] >>> INICIANDO EMPRESA: {empresa}", consola=True)
    
    driver = configurar_driver(dir_hilo)
    wait = WebDriverWait(driver, 45)
    
    try:
        if not realizar_login(driver, wait, usuario, password):
            escribir_log(f"[HILO {id_hilo}] ERROR CRÍTICO: Fallo de login para {empresa}", consola=True)
            return {"status": "ERROR_LOGIN", "empresa": empresa}

        for tarea in tareas_base:
            categoria_raw = tarea['prefijo']
            categoria_clean = limpiar_nombre_archivo(categoria_raw)
            exito_enlace = False
            
            for intento in range(1, 3):
                try:
                    ts = datetime.now().strftime('%H%M%S_%f')[:-3]
                    # Preparar rutas
                    dir_destino_excel = os.path.join(ruta_excel_base, categoria_clean)
                    dir_destino_csv = os.path.join(ruta_csv_base, categoria_clean)
                    os.makedirs(dir_destino_excel, exist_ok=True)
                    os.makedirs(dir_destino_csv, exist_ok=True)

                    filtro_csv = filtros_proyectos.get(empresa, "")
                    empresa_encoded = urllib.parse.quote(empresa.strip(), safe="", encoding="utf-8")
                    url_final = tarea['url'].strip().replace("empresas.txt", empresa_encoded).replace("Proyecto a borrar.csv", filtro_csv)

                    navegar_y_preparar_descarga(driver, wait, url_final)
                    
                    archivo = None
                    inicio_espera = time.time()
                    while time.time() - inicio_espera < 180:
                        files = [f for f in os.listdir(dir_hilo) if not f.endswith(('.crdownload', '.tmp'))]
                        if files:
                            temp_path = os.path.join(dir_hilo, files[0])
                            if os.path.getsize(temp_path) > 500 and archivo_estable(temp_path):
                                archivo = temp_path
                                break
                        time.sleep(2)

                    if archivo:
                        nombre_unico = f"{clean_emp}_{categoria_clean}_{ts}"
                        ruta_excel = os.path.join(dir_destino_excel, nombre_unico + ".xlsx")
                        shutil.move(archivo, ruta_excel)
                        
                        try:
                            df_raw = pd.read_excel(ruta_excel, engine="calamine")
                        except:
                            df_raw = pd.read_excel(ruta_excel, engine="openpyxl")

                        # --- CAMBIO AQUÍ: Log de Vacío o Éxito ---
                        if len(df_raw) > 0:
                            df_final = transformar_datos_powerquery(df_raw, categoria_raw, empresa)
                            if df_final is not None:
                                ruta_csv_final = os.path.join(dir_destino_csv, nombre_unico + ".csv")
                                df_final.to_csv(ruta_csv_final, sep=";", index=False, encoding="utf-8-sig")
                                escribir_log(f"[HILO {id_hilo}] [OK] Descarga completa: {categoria_raw} ({len(df_final)} filas)")
                        else:
                            # Esto es lo que te faltaba para saber qué pasó
                            escribir_log(f"[HILO {id_hilo}] [AVISO] El fichero '{categoria_raw}' estaba VACÍO en Business Central.")
                        
                        exito_enlace = True
                        break 
                    else:
                        raise Exception("El archivo no apareció en disco")

                except Exception as e:
                    escribir_log(f"[HILO {id_hilo}] Error en intento {intento} ({categoria_raw}): {e}")
                    driver.refresh()
                    time.sleep(3)

            if not exito_enlace:
                escribir_log(f"[HILO {id_hilo}] [FALLO FINAL] No se pudo obtener {categoria_raw}", consola=True)

    except Exception as e:
        escribir_log(f"[HILO {id_hilo}] ERROR FATAL en empresa {empresa}: {e}", consola=True)
    finally:
        driver.quit()
        shutil.rmtree(dir_hilo, ignore_errors=True)
        escribir_log(f"[HILO {id_hilo}] <<< FINALIZADA EMPRESA: {empresa} | Duración: {datetime.now() - inicio_empresa}", consola=True)
    
    return {"status": "FINISHED", "empresa": empresa}

# fin descargas

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
    
    # --- MODIFICACIÓN: Limpieza carpeta destino final ---
    print(f"[*] Preparando carpeta destino: {ruta_csv_project}")
    limpiar_directorio_recursivo(ruta_csv_project)
    # ----------------------------------------------------

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
                
                # --- MODIFICACIÓN: Guardar en carpeta csvProject ---
                ruta_salida = os.path.join(ruta_csv_project, nombre_final)
                # -------------------------------------------------
                
                # Usamos quoting=csv.QUOTE_MINIMAL para asegurar que si hay comas internas no rompa nada
                df_consolidado.to_csv(ruta_salida, index=False, sep=";", encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
                
                print(f"      [OK] Generado: {nombre_final} ({len(df_consolidado)} filas) en csvProject")
                total_consolidados += 1
            except Exception as e:
                print(f"      [ERROR] Fallo en '{carpeta_cat}': {e}")
        else:
            print(f"    > Omitiendo '{carpeta_cat}': Vacía.")
    print(f"[*] Consolidación finalizada. {total_consolidados} maestros creados.")


if __name__ == "__main__":
    inicializar_entorno()
    inicio_global = datetime.now()
    print(f"=== INICIO PROCESO POR EMPRESA: {inicio_global.strftime('%H:%M:%S')} ===")

    try:
        # 1. Carga de credenciales (Usuario en línea 1, Pass en línea 2)
        if not os.path.exists(ruta_usuario_txt):
            raise FileNotFoundError(f"No existe el archivo de credenciales en {ruta_usuario_txt}")
            
        with open(ruta_usuario_txt, "r", encoding="utf-8") as f:
            datos_login = [l.strip() for l in f.readlines() if l.strip()]
        
        # 2. Carga de Listas
        empresas = sorted(list(set([l.strip() for l in open(ruta_empresas_txt, "r", encoding="utf-8") if l.strip()])))
        enlaces_raw = [l.strip() for l in open(ruta_enlaces_txt, "r", encoding="utf-8") if l.strip()]

        # 3. Limpieza de carpetas de trabajo
        limpiar_directorio_recursivo(ruta_excel_base)
        limpiar_directorio_recursivo(ruta_csv_base)

        # 4. Preparación de Filtros de Proyectos
        filtros_proyectos = {}
        if os.path.exists(ruta_csv_proyectos):
            try:
                df_filtros = pd.read_csv(ruta_csv_proyectos, sep=None, engine='python', encoding='utf-8-sig')
                for _, row in df_filtros.iterrows():
                    emp = str(row["EMPRESA"]).strip()
                    proy = str(row["PROYECTOS A ELIMINAR"]).strip()
                    if emp and proy: 
                        filtros_proyectos.setdefault(emp, []).append(proy)
            except Exception as e:
                escribir_log(f"Error cargando filtros de proyectos: {e}")

        # Formatear filtros para la URL de Business Central
        sep_url = "%26%3c%3e"
        for emp in filtros_proyectos:
            filtros_proyectos[emp] = sep_url.join(filtros_proyectos[emp])

        # 5. Estructurar tareas base (Agrupando URL y Prefijo de 2 en 2)
        tareas_base = []
        for i in range(0, len(enlaces_raw) - 1, 2):
            tareas_base.append({
                "url": enlaces_raw[i], 
                "prefijo": enlaces_raw[i+1]
            })

        # 6. Ejecución Paralela: Un hilo por Empresa (Máximo 3 empresas a la vez)
        print(f"[*] Iniciando descarga de {len(empresas)} empresas con {len(tareas_base)} enlaces cada una...")
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futuros = []
            for idx, emp_nombre in enumerate(empresas):
                futuros.append(executor.submit(
                    procesar_empresa_completa, 
                    idx + 1,           # ID de hilo
                    emp_nombre,        # Nombre de la empresa
                    datos_login[0],    # Usuario
                    datos_login[1],    # Password
                    tareas_base,       # Lista de diccionarios con URL/Prefijo
                    filtros_proyectos  # Diccionario de filtros
                ))
            
            # Esperar a que todos los hilos finalicen
            for f in futuros:
                try:
                    f.result()
                except Exception as e:
                    print(f"Error en ejecución de hilo: {e}")

        # 7. Consolidación Final de CSVs
        consolidar_archivos_por_categoria()
        
        tiempo_total = datetime.now() - inicio_global
        print(f"\n=== FIN DEL PROCESO ===")
        print(f"Tiempo total: {tiempo_total}")
        escribir_log(f"PROCESO FINALIZADO. Tiempo total: {tiempo_total}", consola=True)

    except Exception as e:
        print(f"[FATAL MAIN] {e}")
        escribir_log(f"FATAL MAIN: {e}")