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
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import win32com.client

# Intento de importar calamine para asegurar que pandas lo detecte si es necesario
try:
    import python_calamine
except ImportError:
    pass

# ==============================================================================
# --- CONFIGURACIÓN DE RUTAS DINÁMICAS ---
# ==============================================================================

# Detectar la carpeta donde reside este script .py
DIRECTORIO_BASE = os.path.dirname(os.path.abspath(__file__))

# Rutas de Archivos de Configuración (Ahora relativas al script)
ruta_usuario_txt = os.path.join(DIRECTORIO_BASE, "usuarioContraseña.txt")
ruta_descarga_txt = os.path.join(DIRECTORIO_BASE, "carpetaChromeDescargas.txt")
ruta_enlaces_txt = os.path.join(DIRECTORIO_BASE, "enlaces.txt")
ruta_empresas_txt = os.path.join(DIRECTORIO_BASE, "Empresas.txt")
ruta_csv_proyectos = os.path.join(DIRECTORIO_BASE, "Proyecto a borrar.csv")
ruta_dp_responsable = os.path.join(DIRECTORIO_BASE, "DP_RESPONSABLE.xlsx")
ruta_log = os.path.join(DIRECTORIO_BASE, "log_proceso.txt")
ruta_log_enlaces = os.path.join(DIRECTORIO_BASE, "debug_enlaces.txt")

# Usaremos el mismo log_lock que ya tienes definido o uno nuevo
enlaces_lock = threading.Lock()

# Carpeta principal de resultados (dentro de la carpeta del script)
ruta_base_bc = os.path.join(DIRECTORIO_BASE, "ArchivosBC")

# Subcarpetas de trabajo
ruta_excel_base = os.path.join(ruta_base_bc, "Excel")
ruta_csv_base = os.path.join(ruta_base_bc, "CSV")
ruta_csv_project = os.path.join(ruta_base_bc, "csvProject")
ruta_errores = os.path.join(ruta_base_bc, "Errores")

# Fichero para la ruta del Excel final a actualizar
ruta_actualizar_excel_txt = os.path.join(DIRECTORIO_BASE, "actualizarExcel.txt")

# Carpeta temporal para los hilos (dentro de la carpeta del script)
dir_base_hilos = os.path.join(DIRECTORIO_BASE, "Temp_Workers")

# ==============================================================================

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


def registrar_enlace_intento(empresa, categoria, url):
    linea = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Empresa: {empresa} | Cat: {categoria} | URL: {url}\n"
    with enlaces_lock:
        with open(ruta_log_enlaces, "a", encoding="utf-8") as f:
            f.write(linea)

def limpiar_nombre_archivo(nombre):
    limpio = re.sub(r'[\\/*?:"<>|]', "", nombre)
    return limpio.strip()

def inicializar_entorno():
    """Crea estructura, limpia temporales y carga tablas auxiliares."""
    global df_responsables_global
    
    # 1. RESET DE LOGS: Limpiamos los archivos para que no sean acumulativos
    # Al abrir en modo 'w' eliminamos el contenido previo de ejecuciones pasadas
    for archivo_log in [ruta_log, ruta_log_enlaces]:
        with open(archivo_log, "w", encoding="utf-8") as f:
            f.write(f"--- INICIO DE PROCESO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n")
    
    # 2. GESTIÓN DE DIRECTORIOS: Crear carpetas necesarias
    carpetas = [ruta_base_bc, ruta_excel_base, ruta_csv_base, ruta_errores, ruta_csv_project]
    for carpeta in carpetas:
        os.makedirs(carpeta, exist_ok=True)
    
    # Limpiar temporales de hilos de ejecuciones anteriores
    if os.path.exists(dir_base_hilos):
        shutil.rmtree(dir_base_hilos, ignore_errors=True)
    os.makedirs(dir_base_hilos, exist_ok=True)

    # 3. CARGA DE TABLAS MAESTRAS
    escribir_log("Cargando tabla maestra de Responsables...")
    try:
        if os.path.exists(ruta_dp_responsable):
            # Prioridad motor calamine para velocidad
            try:
                df_resp = pd.read_excel(ruta_dp_responsable, engine="calamine")
            except Exception:
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
    chrome_options.add_argument("--headless=new")
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

def navegar_y_preparar_descarga(driver, wait, url_final, id_hilo, reintentos=3):
    """
    Gestiona la navegación localizando el primer iframe disponible de forma dinámica.
    """
    for intento in range(1, reintentos + 1):
        try:
            driver.get(url_final)
            escribir_log(f"[HILO {id_hilo}] Cargando URL, buscando iframe dinámicamente...", consola=True)

            # Esperar a que aparezca al menos un iframe y cambiar a él automáticamente
            # EC.frame_to_be_available_and_switch_to_it busca por locator, 
            # usando (By.TAG_NAME, "iframe") capturamos el primero que se renderice.
            try:
                wait.until(EC.frame_to_be_available_and_switch_to_it((By.TAG_NAME, "iframe")))
                escribir_log(f"[HILO {id_hilo}] Iframe detectado y switch realizado.", consola=True)
            except Exception:
                escribir_log(f"[HILO {id_hilo}] Tiempo de espera agotado buscando iframe.", consola=True)
                if intento < reintentos:
                    driver.refresh()
                    continue
                return False

            # --- Localización de botones dentro del iframe ---
            
            # 1. Botón "Abrir en Excel"
            # Usamos el XPath dinámico que mencionaste anteriormente como prioridad
            xpath_menu = "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button"
            try:
                btn_menu = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_menu)))
            except:
                # Fallback: buscar por el título o texto del botón si el XPath absoluto falla
                btn_menu = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@title, 'Abrir en Excel')]")))
            
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn_menu)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", btn_menu)

            # 2. Botón final de descarga en el desplegable
            xpath_descarga = "/html/body/div[1]/div[2]/form/div/div[2]/div[5]/div/div/div/div[3]/div/div/ul/li/button"
            btn_desc = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_descarga)))
            driver.execute_script("arguments[0].click();", btn_desc)
            
            return True 

        except Exception as e:
            escribir_log(f"[HILO {id_hilo}] Error en navegación (Intento {intento}): {str(e)}", consola=True)
            if intento < reintentos:
                driver.refresh()
                time.sleep(5)

    return False

def procesar_empresa_completa(id_hilo, empresa, usuario, password, tareas_base, filtros_proyectos, max_intentos_empresa=2):
    inicio_empresa = datetime.now()
    clean_emp = limpiar_nombre_archivo(empresa).replace(" ", "_")
    dir_hilo = os.path.join(dir_base_hilos, f"worker_emp_{id_hilo}")
    os.makedirs(dir_hilo, exist_ok=True)

    escribir_log(f"[HILO {id_hilo}] >>> INICIANDO EMPRESA: {empresa}", consola=True)

    for intento_empresa in range(1, max_intentos_empresa + 1):
        driver = None
        try:
            driver = configurar_driver(dir_hilo)
            driver.set_page_load_timeout(300)  # Timeout extenso por si BC va lento
            wait = WebDriverWait(driver, 60)

            if not realizar_login(driver, wait, usuario, password):
                raise Exception(f"Fallo login en {empresa}")

            for tarea in tareas_base:
                categoria_raw = tarea["prefijo"]
                categoria_clean = limpiar_nombre_archivo(categoria_raw)
                exito_enlace = False

                # Preparar rutas de salida
                dir_destino_excel = os.path.join(ruta_excel_base, categoria_clean)
                dir_destino_csv = os.path.join(ruta_csv_base, categoria_clean)
                os.makedirs(dir_destino_excel, exist_ok=True)
                os.makedirs(dir_destino_csv, exist_ok=True)

                # --- CONSTRUCCIÓN DE LA URL ---
                filtro_url = filtros_proyectos.get(empresa, None)  # None si no hay proyectos
                empresa_encoded = urllib.parse.quote(empresa.strip(), safe="", encoding="utf-8")
                url_base = tarea["url"].strip().replace("empresas.txt", empresa_encoded)

                if filtro_url and filtro_url.strip():  # Caso normal: hay proyectos
                    url_final = url_base.replace("Proyecto a borrar.csv", filtro_url)
                else:  # Caso sin proyectos: eliminar la parte Job No. del filter
                    # Eliminar exactamente la parte que genera problemas
                    url_final = re.sub(
                        r"%27Job%20Ledger%20Entry%27\.%27Job%20No\.%27%20IS%20%27%3c%3eProyecto a borrar\.csv%27%20AND%20",
                        "",
                        url_base
                    )

                # Registro de debug para detectar errores en Italia u otras
                registrar_enlace_intento(empresa, categoria_raw, url_final)

                escribir_log(f"[HILO {id_hilo}] INICIO TAREA: {categoria_raw}", consola=True)

                # Intentos por cada tarea (enlace)
                for intento_tarea in range(1, 3):
                    try:
                        ts = datetime.now().strftime("%H%M%S_%f")[:-3]

                        if not navegar_y_preparar_descarga(driver, wait, url_final, id_hilo):
                            raise Exception("No se pudo interactuar con el botón de descarga")

                        escribir_log(f"[HILO {id_hilo}] Esperando Excel (Máx. 1h)...", consola=True)

                        archivo_descargado = None
                        inicio_espera = time.time()
                        tiempo_maximo_generacion = 3600 

                        # Bucle de espera del archivo físico
                        while time.time() - inicio_espera < tiempo_maximo_generacion:
                            archivos = os.listdir(dir_hilo)

                            # Si hay descarga activa, reseteamos el timeout
                            if any(f.endswith(".crdownload") or f.endswith(".tmp") for f in archivos):
                                inicio_espera = time.time()
                                time.sleep(10)
                                continue

                            xlsx = [f for f in archivos if f.endswith(".xlsx") and not f.startswith("~$")]

                            if xlsx:
                                temp_path = os.path.join(dir_hilo, xlsx[0])
                                if os.path.getsize(temp_path) > 1024 and archivo_estable(temp_path):
                                    archivo_descargado = temp_path
                                    break
                            time.sleep(5)

                        if not archivo_descargado:
                            raise Exception("Timeout: El servidor no envió el archivo")

                        # Mover y procesar
                        nombre_unico = f"{clean_emp}_{categoria_clean}_{ts}"
                        ruta_excel_final = os.path.join(dir_destino_excel, nombre_unico + ".xlsx")
                        
                        time.sleep(2)
                        shutil.move(archivo_descargado, ruta_excel_final)

                        # Lectura de datos
                        try:
                            df_raw = pd.read_excel(ruta_excel_final, engine="calamine")
                        except:
                            df_raw = pd.read_excel(ruta_excel_final, engine="openpyxl")

                        if len(df_raw) > 0:
                            df_transformado = transformar_datos_powerquery(df_raw, categoria_raw, empresa)
                            if df_transformado is not None:
                                ruta_csv_final = os.path.join(dir_destino_csv, nombre_unico + ".csv")
                                df_transformado.to_csv(ruta_csv_final, sep=";", index=False, encoding="utf-8-sig")
                                escribir_log(f"[HILO {id_hilo}] OK '{categoria_raw}' -> {len(df_transformado)} filas.", consola=True)
                        else:
                            escribir_log(f"[HILO {id_hilo}] ADVERTENCIA: El Excel para {empresa} llegó VACÍO.", consola=True)

                        exito_enlace = True
                        break 

                    except Exception as e_tarea:
                        escribir_log(f"[HILO {id_hilo}] ERROR TAREA {categoria_raw}: {e_tarea}", consola=True)
                        if intento_tarea < 2:
                            driver.refresh()
                            time.sleep(10)

                if not exito_enlace:
                    escribir_log(f"[HILO {id_hilo}] FALLO DEFINITIVO EN TAREA: {categoria_raw}", consola=True)

            return {"status": "FINISHED", "empresa": empresa}

        except Exception as e_emp:
            escribir_log(f"[HILO {id_hilo}] ERROR CRÍTICO EN EMPRESA '{empresa}': {e_emp}", consola=True)
            if intento_empresa < max_intentos_empresa:
                time.sleep(15)  # Pausa antes de reintentar la empresa

        finally:
            if driver:
                driver.quit()
            if os.path.exists(dir_hilo):
                shutil.rmtree(dir_hilo, ignore_errors=True)

    return {"status": "ERROR_FINAL", "empresa": empresa}

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
          
                    temp_df = pd.read_csv(f, sep=";", encoding="utf-8-sig", dtype=str, on_bad_lines='skip')
                    
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

#actualizar excel
def actualizar_excel_powerquery(ruta_excel):

    try:
        escribir_log(f"Iniciando actualización de Power Query en: {ruta_excel}", consola=True)
        
        # Iniciar instancia de Excel
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False  # Mantenerlo oculto
        excel.DisplayAlerts = False # Evitar mensajes de confirmación
        
        # Abrir el libro
        wb = excel.Workbooks.Open(ruta_excel)
        
        # Refrescar todas las conexiones (Power Query)
        # BackgroundQuery=False es vital para que Python espere a que termine de cargar
        for conn in wb.Queries:
            pass # Solo para verificar que existen queries si fuera necesario
            
        wb.RefreshAll()
        
        # Opcional: Si tus conexiones tienen activada la "Actualización en segundo plano", 
        # RefreshAll() vuelve inmediatamente. Para forzar la espera:
        excel.CalculateUntilAsyncQueriesDone()
        
        wb.Save()
        wb.Close()
        excel.Quit()
        
        escribir_log(f"Excel actualizado y guardado correctamente: {ruta_excel}", consola=True)
        return True
    except Exception as e:
        escribir_log(f"ERROR actualizando Excel: {e}", consola=True)
        if 'excel' in locals(): excel.Quit()
        return False
#fin actualizar excel

if __name__ == "__main__":
    # 1. Configuración inicial y logs
    inicializar_entorno()
    inicio_global = datetime.now()
    
    # 2. Localización del informe final a actualizar
    ruta_informe_final = None
    if os.path.exists(ruta_actualizar_excel_txt):
        try:
            with open(ruta_actualizar_excel_txt, "r", encoding="utf-8") as f:
                linea = f.readline().strip()
                if linea:
                    ruta_informe_final = linea.replace('"', '')
                    escribir_log(f"Ruta de informe cargada: {ruta_informe_final}", consola=True)
        except Exception as e:
            escribir_log(f"Error leyendo actualizarExcel.txt: {e}", consola=True)
    else:
        escribir_log("AVISO: No se encontró 'actualizarExcel.txt'.", consola=True)

    print(f"=== INICIO PROCESO (Ruta Base: {DIRECTORIO_BASE}) ===")
    print(f"Hora de inicio: {inicio_global.strftime('%H:%M:%S')}")

    try:
        # 3. Carga de credenciales y parámetros
        if not os.path.exists(ruta_usuario_txt):
            raise FileNotFoundError(f"Falta 'usuarioContraseña.txt'")
            
        with open(ruta_usuario_txt, "r", encoding="utf-8") as f:
            lineas = [l.strip() for l in f.readlines() if l.strip()]
            if len(lineas) < 2:
                raise ValueError("Credenciales incompletas en el archivo TXT.")
            usuario_bc, password_bc = lineas[0], lineas[1]
        
        empresas = sorted(list(set([l.strip() for l in open(ruta_empresas_txt, "r", encoding="utf-8") if l.strip()])))
        enlaces_raw = [l.strip() for l in open(ruta_enlaces_txt, "r", encoding="utf-8") if l.strip()]

        # 4. Limpieza de carpetas de trabajo (Excel y CSV temporales)
        limpiar_directorio_recursivo(ruta_excel_base)
        limpiar_directorio_recursivo(ruta_csv_base)

        # 5. Preparación de Filtros de Proyectos (Job No.)
        filtros_proyectos = {}
        if os.path.exists(ruta_csv_proyectos):
            try:
                # Lectura automática del delimitador (coma o punto y coma)
                df_filtros = pd.read_csv(ruta_csv_proyectos, sep=None, engine='python', encoding='utf-8-sig')
                for _, row in df_filtros.iterrows():
                    emp = str(row.iloc[0]).strip()
                    proy = str(row.iloc[1]).strip()
                    if emp and proy: 
                        filtros_proyectos.setdefault(emp, []).append(proy)
                
                # Formatear filtros para la URL de Business Central
                sep_url = "%26%3c%3e" # Representa el operador &<>
                for emp in filtros_proyectos:
                    filtros_proyectos[emp] = sep_url.join(filtros_proyectos[emp])
            except Exception as e:
                escribir_log(f"Error procesando CSV de filtros: {e}")

        # 6. Mapeo de Tareas (URL + Categoría)
        tareas_base = [{"url": enlaces_raw[i], "prefijo": enlaces_raw[i+1]} 
                       for i in range(0, len(enlaces_raw) - 1, 2)]

        # 7. Ejecución Paralela con ThreadPoolExecutor
        print(f"[*] Lanzando hilos para {len(empresas)} empresas...")
        # Nota: max_workers=3 es ideal para no saturar la sesión de BC
        with ThreadPoolExecutor(max_workers=3) as executor:
            futuros = {
                executor.submit(
                    procesar_empresa_completa, 
                    idx + 1, emp, usuario_bc, password_bc, tareas_base, filtros_proyectos
                ): emp for idx, emp in enumerate(empresas)
            }

            for fut in futuros:
                emp_nombre = futuros[fut]
                try:
                    resultado = fut.result()
                    escribir_log(f"Hilo {emp_nombre} finalizado: {resultado.get('status')}", consola=True)
                except Exception as e:
                    escribir_log(f"[ERROR CRÍTICO] {emp_nombre} falló: {e}", consola=True)

        # 8. Consolidación de archivos (Generar CSVs únicos por categoría)
        consolidar_archivos_por_categoria()
        
        # 9. Actualización Automática de Power Query
        if ruta_informe_final and os.path.exists(ruta_informe_final):
            escribir_log(f"Actualizando Excel final...", consola=True)
            if actualizar_excel_powerquery(ruta_informe_final):
                escribir_log("Excel actualizado correctamente.", consola=True)
            else:
                escribir_log("Error en la actualización de Power Query.", consola=True)

        # 10. Cierre y Métricas
        tiempo_total = datetime.now() - inicio_global
        print(f"\n=== PROCESO FINALIZADO EN: {tiempo_total} ===")
        escribir_log(f"EJECUCIÓN EXITOSA. Total: {tiempo_total}", consola=True)

    except Exception as e:
        escribir_log(f"ERROR FATAL EN MAIN: {e}", consola=True)
        traceback.print_exc()