# -*- coding: utf-8 -*-
import os
import time
import shutil
import urllib.parse
import csv
import sys
import re
import glob
import pandas as pd
import traceback
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import win32com.client

# Intento de importar calamine (opcional)
try:
    import python_calamine
except Exception:
    pass

# =======================================================================
# --- CONFIGURACIÓN DE RUTAS DINÁMICAS ---
# =======================================================================

DIRECTORIO_BASE = os.path.dirname(os.path.abspath(__file__))

ruta_usuario_txt = os.path.join(DIRECTORIO_BASE, "usuarioContraseña.txt")
ruta_descarga_txt = os.path.join(DIRECTORIO_BASE, "carpetaChromeDescargas.txt")
ruta_enlaces_txt = os.path.join(DIRECTORIO_BASE, "enlaces.txt")
ruta_empresas_txt = os.path.join(DIRECTORIO_BASE, "Empresas.txt")
ruta_csv_proyectos = os.path.join(DIRECTORIO_BASE, "Proyecto a borrar.csv")
ruta_dp_responsable = os.path.join(DIRECTORIO_BASE, "DP_RESPONSABLE.xlsx")
ruta_log = os.path.join(DIRECTORIO_BASE, "log_proceso.txt")

ruta_base_bc = os.path.join(DIRECTORIO_BASE, "ArchivosBC")
ruta_excel_base = os.path.join(ruta_base_bc, "Excel")
ruta_csv_base = os.path.join(ruta_base_bc, "CSV")
ruta_csv_project = os.path.join(ruta_base_bc, "csvProject")
ruta_errores = os.path.join(ruta_base_bc, "Errores")

ruta_actualizar_excel_txt = os.path.join(DIRECTORIO_BASE, "actualizarExcel.txt")

# Carpeta temporal single-worker
dir_base_hilos = os.path.join(DIRECTORIO_BASE, "Temp_Workers")
dir_worker = os.path.join(dir_base_hilos, "worker_single")

# =======================================================================
# --- GLOBALS ---
# =======================================================================

df_responsables_global = None

# =======================================================================
# --- UTILIDADES ---
# =======================================================================

def limpiar_nombre_archivo(nombre):
    limpio = re.sub(r'[\\/*?:"<>|]', "", str(nombre))
    return limpio.strip()

def escribir_log(mensaje, consola=False):
    linea = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}"
    try:
        with open(ruta_log, "a", encoding="utf-8") as log:
            log.write(linea + "\n")
    except:
        pass
    if consola:
        print(linea)

def inicializar_entorno():
    """Crea estructura, limpia temporales y carga tablas auxiliares."""
    global df_responsables_global

    for carpeta in [ruta_base_bc, ruta_excel_base, ruta_csv_base, ruta_errores, ruta_csv_project]:
        os.makedirs(carpeta, exist_ok=True)

    if os.path.exists(dir_base_hilos):
        shutil.rmtree(dir_base_hilos, ignore_errors=True)
    os.makedirs(dir_worker, exist_ok=True)

    escribir_log("Cargando tabla maestra de Responsables...")
    try:
        if os.path.exists(ruta_dp_responsable):
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

def esperar_pagina_cargada(driver, timeout=40):
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        return True
    except Exception:
        return False

def archivo_estable(ruta, intentos=5, espera=2):
    if ruta.endswith('.crdownload') or ruta.endswith('.tmp'):
        return False
    tam_anterior = -1
    for _ in range(intentos):
        if not os.path.exists(ruta):
            time.sleep(espera)
            continue
        try:
            tam_actual = os.path.getsize(ruta)
            if tam_actual > 0 and tam_actual == tam_anterior:
                # intento de abrir para asegurar que no haya lock
                with open(ruta, "rb"):
                    return True
            tam_anterior = tam_actual
        except Exception:
            pass
        time.sleep(espera)
    return False

def realizar_login(driver, wait, usuario, password, reintentos=3):
    """Intenta loguear; devuelve True si OK."""
    for intento in range(1, reintentos + 1):
        try:
            driver.get("https://bc.zener.es/ZENER_BC/")
            if not esperar_pagina_cargada(driver):
                continue

            user_field = wait.until(EC.element_to_be_clickable((By.ID, "UserName")))
            user_field.clear()
            user_field.send_keys(usuario)

            pass_field = driver.find_element(By.ID, "Password")
            pass_field.clear()
            pass_field.send_keys(password)

            driver.find_element(By.ID, "submitButton").click()
            wait.until(EC.url_contains("ZENER_BC"))
            escribir_log(f"Login exitoso (intento {intento}).", consola=True)
            return True
        except Exception as e:
            escribir_log(f"Error de login (intento {intento}): {e}", consola=True)
            try:
                driver.delete_all_cookies()
                driver.refresh()
            except Exception:
                pass
            time.sleep(2)
    return False

def transformar_datos_powerquery(df, categoria, empresa):
    try:
        df.columns = df.columns.str.strip()
        df["EMPRESA"] = empresa
        es_certificacion = "certificac" in categoria.lower()

        def safe_num(series):
            if series is None:
                return 0
            s = series.astype(str).str.strip().replace(r'[\u00a0 ]', '', regex=True)
            s = s.apply(lambda x: x.replace('.', '').replace(',', '.') if ',' in x else x)
            return pd.to_numeric(s, errors="coerce").fillna(0)

        if not es_certificacion:
            renames = {"COD. DP": "DP", "Precio venta (DL)": "Cantidad producción actual"}
        else:
            renames = {"COD. DP": "DP", "Importe producción actual venta (DL)": "PRODUCCIÓN"}

        df = df.rename(columns=renames)

        if not es_certificacion:
            df["FACTURACIÓN"] = -safe_num(df["Importe línea (DL)"]) if "Importe línea (DL)" in df.columns else 0
        else:
            df["PRODUCCIÓN"] = safe_num(df.get("PRODUCCIÓN", 0))
            df["Tipo movimiento"] = "Producción"

        if "DP" in df.columns and df_responsables_global is not None:
            df["DP"] = df["DP"].astype(str).str.strip()
            df = df.merge(df_responsables_global, left_on="DP", right_on="DP_KEY", how="left")

        return df.copy()

    except Exception as e:
        escribir_log(f"Error en transformación {categoria}: {e}")
        return None

def configurar_driver(dir_hilo, headless=True):
    """Configura las opciones de Chrome y el comportamiento de descarga."""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": dir_hilo,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    })
    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": dir_hilo})
    except Exception:
        pass
    return driver

def navegar_y_preparar_descarga(driver, wait, url_final, etiqueta, reintentos=3, timeout_iframe=20):
    """
    Gestiona la navegación localizando el primer iframe disponible de forma dinámica.
    etiqueta: cadena para logging (por ejemplo empresa o hilo)
    """
    for intento in range(1, reintentos + 1):
        try:
            driver.get(url_final)
            escribir_log(f"[{etiqueta}] Cargando URL, buscando iframe (intento {intento})...", consola=True)

            try:
                wait.until(EC.frame_to_be_available_and_switch_to_it((By.TAG_NAME, "iframe")))
                escribir_log(f"[{etiqueta}] Iframe detectado y switch realizado.", consola=True)
            except Exception:
                escribir_log(f"[{etiqueta}] Tiempo de espera agotado buscando iframe.", consola=True)
                if intento < reintentos:
                    driver.refresh()
                    continue
                return False

            # Botón "Abrir en Excel"
            xpath_menu = "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button"
            try:
                btn_menu = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_menu)))
            except Exception:
                btn_menu = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@title, 'Abrir en Excel')]")))

            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn_menu)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", btn_menu)

            # Botón final de descarga del desplegable
            xpath_descarga = "/html/body/div[1]/div[2]/form/div/div[2]/div[5]/div/div/div/div[3]/div/div/ul/li/button"
            btn_desc = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_descarga)))
            driver.execute_script("arguments[0].click();", btn_desc)

            # volver al contexto por si acaso
            driver.switch_to.default_content()
            return True

        except Exception as e:
            escribir_log(f"[{etiqueta}] Error en navegación (Intento {intento}): {str(e)}", consola=True)
            try:
                driver.switch_to.default_content()
            except Exception:
                pass
            if intento < reintentos:
                try:
                    driver.refresh()
                except Exception:
                    pass
                time.sleep(3)
            else:
                return False

    return False

def limpiar_directorio_recursivo(directorio_base):
    if os.path.exists(directorio_base):
        shutil.rmtree(directorio_base)
    os.makedirs(directorio_base)

def limpiar_columnas_maestro(df):
    df = df.fillna("")
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = (
                df[col].astype(str)
                      .str.replace(';', ',', regex=False)
                      .str.replace('"', "'", regex=False)
                      .str.replace(r'[\n\r]+', ' ', regex=True)
                      .str.strip()
            )
    df = df[df.astype(str).ne("").any(axis=1)]
    return df

def consolidar_archivos_por_categoria():
    escribir_log("Iniciando consolidación de archivos por categoría...", consola=True)
    if not os.path.exists(ruta_csv_base):
        escribir_log("No existe la ruta base de CSV, saltando consolidación.")
        return

    subcarpetas = [f for f in os.listdir(ruta_csv_base) if os.path.isdir(os.path.join(ruta_csv_base, f))]

    for carpeta_cat in subcarpetas:
        ruta_cat_csv = os.path.join(ruta_csv_base, carpeta_cat)
        archivos_csv = glob.glob(os.path.join(ruta_cat_csv, "*.csv"))

        if archivos_csv:
            try:
                df_list = []
                for f in archivos_csv:
                    temp_df = pd.read_csv(f, sep=";", encoding="utf-8-sig", dtype=str, on_bad_lines='skip')
                    if not temp_df.empty:
                        df_list.append(temp_df)

                if df_list:
                    df_consolidado = pd.concat(df_list, ignore_index=True, sort=False)
                    df_consolidado = limpiar_columnas_maestro(df_consolidado)
                    ruta_salida = os.path.join(ruta_csv_project, f"{carpeta_cat} Unidos.csv")
                    df_consolidado.to_csv(ruta_salida, index=False, sep=";", encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
                    escribir_log(f"[OK] Generado: {carpeta_cat} Unidos.csv ({len(df_consolidado)} filas)", consola=True)
            except Exception as e:
                escribir_log(f"[ERROR] en consolidación de '{carpeta_cat}': {e}")

def actualizar_excel_powerquery(ruta_excel):
    try:
        escribir_log(f"Iniciando actualización de Power Query en: {ruta_excel}", consola=True)
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False

        wb = excel.Workbooks.Open(ruta_excel)
        wb.RefreshAll()
        excel.CalculateUntilAsyncQueriesDone()
        wb.Save()
        wb.Close()
        excel.Quit()
        escribir_log(f"Excel actualizado y guardado correctamente: {ruta_excel}", consola=True)
        return True
    except Exception as e:
        escribir_log(f"ERROR actualizando Excel: {e}", consola=True)
        try:
            excel.Quit()
        except Exception:
            pass
        return False

# -----------------------------------------------------------------------
# Función principal por empresa en modo secuencial (usa el driver existente)
# -----------------------------------------------------------------------
def procesar_empresa_secuencial(driver, wait, empresa, usuario, password, tareas_base, filtros_proyectos):
    """
    Procesa todas las tareas para una empresa usando el driver proporcionado.
    Si detecta que necesita relogin/recrear navegador, devuelve:
        {"status": "RELOGIN_REQUIRED", "empresa": empresa}
    Si finaliza bien:
        {"status": "FINISHED", "empresa": empresa}
    Si ocurre un error crítico:
        {"status": "CRITICAL_ERROR", "empresa": empresa, "error": "..."}
    """
    inicio_empresa = datetime.now()
    clean_emp = limpiar_nombre_archivo(empresa).replace(" ", "_")
    os.makedirs(dir_worker, exist_ok=True)

    escribir_log(f"[{clean_emp}] >>> INICIANDO EMPRESA: {empresa}", consola=True)

    try:
        for tarea in tareas_base:
            categoria_raw = tarea["prefijo"]
            categoria_clean = limpiar_nombre_archivo(categoria_raw)

            dir_destino_excel = os.path.join(ruta_excel_base, categoria_clean)
            dir_destino_csv = os.path.join(ruta_csv_base, categoria_clean)
            os.makedirs(dir_destino_excel, exist_ok=True)
            os.makedirs(dir_destino_csv, exist_ok=True)

            filtro_url = filtros_proyectos.get(empresa, "*")
            empresa_encoded = urllib.parse.quote(empresa.strip(), safe="", encoding="utf-8")
            url_final = tarea["url"].strip().replace("empresas.txt", empresa_encoded).replace("Proyecto a borrar.csv", filtro_url)

            escribir_log(f"[{clean_emp}] PROCESANDO: {categoria_raw}", consola=True)

            exito_tarea = False
            # Dos intentos por tarea inicialmente; si falla la descarga,
            # devolvemos RELOGIN_REQUIRED para que main recree el driver si procede.
            for intento_tarea in range(1, 3):
                try:
                    ts = datetime.now().strftime("%H%M%S_%f")[:-3]

                    etiqueta = f"{clean_emp}-{categoria_clean}"
                    if navegar_y_preparar_descarga(driver, wait, url_final, etiqueta):
                        # Esperar archivo en dir_worker
                        archivo_descargado = None
                        inicio_espera = time.time()
                        while time.time() - inicio_espera < 3600:
                            archivos = os.listdir(dir_worker)
                            if any(f.endswith((".crdownload", ".tmp")) for f in archivos):
                                inicio_espera = time.time()
                                time.sleep(5)
                                continue

                            xlsx = [f for f in archivos if f.endswith(".xlsx") and not f.startswith("~$")]
                            if xlsx:
                                temp_path = os.path.join(dir_worker, xlsx[0])
                                if os.path.getsize(temp_path) > 1024 and archivo_estable(temp_path):
                                    archivo_descargado = temp_path
                                    break
                            time.sleep(3)

                        if archivo_descargado:
                            nombre_unico = f"{clean_emp}_{categoria_clean}_{ts}"
                            ruta_excel_final = os.path.join(dir_destino_excel, nombre_unico + ".xlsx")
                            time.sleep(0.5)
                            shutil.move(archivo_descargado, ruta_excel_final)

                            # Transformación
                            try:
                                df_raw = pd.read_excel(ruta_excel_final, engine="calamine")
                            except Exception:
                                df_raw = pd.read_excel(ruta_excel_final, engine="openpyxl")

                            if not df_raw.empty:
                                df_t = transformar_datos_powerquery(df_raw, categoria_raw, empresa)
                                if df_t is not None:
                                    df_t.to_csv(os.path.join(dir_destino_csv, nombre_unico + ".csv"),
                                               sep=";", index=False, encoding="utf-8-sig")
                            exito_tarea = True
                            break
                        else:
                            raise Exception("No se detectó archivo descargado en tiempo límite.")
                    else:
                        # navegación fallida -> posible sesión rota: pedir relogin a main
                        escribir_log(f"[{clean_emp}] Navegación o interacción fallida (posible sesión).", consola=True)
                        return {"status": "RELOGIN_REQUIRED", "empresa": empresa}

                except Exception as e_intento:
                    escribir_log(f"[{clean_emp}] Intento {intento_tarea} falló para '{categoria_raw}': {e_intento}", consola=True)
                    # si fue primer intento, volvemos a intentar una vez más localmente
                    if intento_tarea == 1:
                        time.sleep(2)
                        continue
                    else:
                        escribir_log(f"[{clean_emp}] ERROR en tarea '{categoria_raw}' tras reintentos.", consola=True)
                        break

            if not exito_tarea:
                escribir_log(f"[{clean_emp}] ERROR: No se pudo descargar {categoria_raw}", consola=True)
            else:
                escribir_log(f"[{clean_emp}] OK: tarea {categoria_raw} procesada.", consola=True)

        tiempo_total_empresa = datetime.now() - inicio_empresa
        escribir_log(f"[{clean_emp}] FIN EMPRESA. Tiempo: {tiempo_total_empresa}", consola=True)
        return {"status": "FINISHED", "empresa": empresa}

    except Exception as e_critico:
        escribir_log(f"[{clean_emp}] ERROR CRÍTICO: {e_critico}", consola=True)
        return {"status": "CRITICAL_ERROR", "empresa": empresa, "error": str(e_critico)}

# ================================
# MAIN (sin multihilo)
# ================================
if __name__ == "__main__":
    inicializar_entorno()
    inicio_global = datetime.now()

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
        escribir_log("AVISO: No se encontró 'actualizarExcel.txt'. No se actualizará el informe final.", consola=True)

    try:
        if not os.path.exists(ruta_usuario_txt):
            raise FileNotFoundError(f"Falta 'usuarioContraseña.txt' en: {ruta_usuario_txt}")

        with open(ruta_usuario_txt, "r", encoding="utf-8") as f:
            lineas = f.readlines()
            if len(lineas) < 2:
                raise ValueError("El archivo de usuario/contraseña no tiene 2 líneas completas.")
            usuario_bc = lineas[0].strip()
            password_bc = lineas[1].strip()

        if not os.path.exists(ruta_empresas_txt) or not os.path.exists(ruta_enlaces_txt):
            raise FileNotFoundError("Faltan archivos de configuración (Empresas.txt o enlaces.txt).")

        empresas = sorted(list(set([l.strip() for l in open(ruta_empresas_txt, "r", encoding="utf-8") if l.strip()])))
        enlaces_raw = [l.strip() for l in open(ruta_enlaces_txt, "r", encoding="utf-8") if l.strip()]

        limpiar_directorio_recursivo(ruta_excel_base)
        limpiar_directorio_recursivo(ruta_csv_base)

        filtros_proyectos = {}
        if os.path.exists(ruta_csv_proyectos):
            try:
                df_filtros = pd.read_csv(ruta_csv_proyectos, sep=None, engine='python', encoding='utf-8-sig')
                for _, row in df_filtros.iterrows():
                    emp = str(row.iloc[0]).strip()
                    proy = str(row.iloc[1]).strip()
                    if emp and proy:
                        filtros_proyectos.setdefault(emp, []).append(proy)
            except Exception as e:
                escribir_log(f"Error cargando filtros de proyectos: {e}")

        sep_url = "%26%3c%3e"
        for emp in filtros_proyectos:
            filtros_proyectos[emp] = sep_url.join(filtros_proyectos[emp])

        tareas_base = []
        for i in range(0, len(enlaces_raw) - 1, 2):
            tareas_base.append({
                "url": enlaces_raw[i],
                "prefijo": enlaces_raw[i+1]
            })

        # Crear navegador único
        escribir_log("Iniciando navegador único para procesamiento secuencial...", consola=True)
        driver = configurar_driver(dir_worker, headless=True)
        driver.set_page_load_timeout(300)
        wait = WebDriverWait(driver, 60)

        # Login inicial
        if not realizar_login(driver, wait, usuario_bc, password_bc):
            escribir_log("ERROR: No se pudo iniciar sesión con las credenciales proporcionadas.", consola=True)
            try:
                driver.quit()
            except Exception:
                pass
            sys.exit(1)

        escribir_log(f"[*] Procesando {len(empresas)} empresas secuencialmente...", consola=True)
        for emp_nombre in empresas:
            # intentos máximos para recrear navegador por empresa
            intents_recreate = 0
            max_recreates = 1
            while True:
                try:
                    resultado = procesar_empresa_secuencial(driver, wait, emp_nombre, usuario_bc, password_bc, tareas_base, filtros_proyectos)
                    status = resultado.get("status")
                    if status == "FINISHED":
                        escribir_log(f"Empresa {emp_nombre} finalizada: {status}", consola=True)
                        break
                    elif status == "RELOGIN_REQUIRED":
                        escribir_log(f"Empresa {emp_nombre} solicita relogin/recreación de navegador.", consola=True)
                        intents_recreate += 1
                        if intents_recreate > max_recreates:
                            escribir_log(f"No se pudo recuperar sesión para {emp_nombre} tras {intents_recreate} intentos. Saltando empresa.", consola=True)
                            break
                        # Recrear navegador en main
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = configurar_driver(dir_worker, headless=True)
                        driver.set_page_load_timeout(300)
                        wait = WebDriverWait(driver, 60)
                        if not realizar_login(driver, wait, usuario_bc, password_bc):
                            escribir_log(f"No se pudo reloguear tras recrear navegador para {emp_nombre}. Saltando empresa.", consola=True)
                            break
                        else:
                            escribir_log(f"Navegador recreado y login OK para {emp_nombre}. Reintentando.", consola=True)
                            continue
                    else:  # CRITICAL_ERROR o inesperado
                        escribir_log(f"[ERROR] Empresa {emp_nombre} terminó con status: {status}. Info: {resultado}", consola=True)
                        break
                except Exception as e:
                    escribir_log(f"[ERROR CRÍTICO] Empresa {emp_nombre} falló con excepción: {e}", consola=True)
                    traceback.print_exc()
                    # Intentamos recrear navegador una vez
                    intents_recreate += 1
                    if intents_recreate > max_recreates:
                        escribir_log(f"No se pudo recuperar tras excepción en {emp_nombre}. Saltando empresa.", consola=True)
                        break
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = configurar_driver(dir_worker, headless=True)
                    driver.set_page_load_timeout(300)
                    wait = WebDriverWait(driver, 60)
                    if not realizar_login(driver, wait, usuario_bc, password_bc):
                        escribir_log(f"No se pudo reloguear tras recrear navegador por excepción en {emp_nombre}. Saltando empresa.", consola=True)
                        break
                    else:
                        escribir_log(f"Navegador recreado y login OK después de excepción en {emp_nombre}. Reintentando.", consola=True)
                        continue

        # Consolidación de archivos
        consolidar_archivos_por_categoria()

        # Actualizar informe Power Query si procede
        if ruta_informe_final and os.path.exists(ruta_informe_final):
            escribir_log(f"Iniciando actualización de Excel: {ruta_informe_final}", consola=True)
            exito = actualizar_excel_powerquery(ruta_informe_final)
            if exito:
                escribir_log("Excel actualizado correctamente.", consola=True)
            else:
                escribir_log("Hubo un problema al actualizar el Excel.", consola=True)

        tiempo_total = datetime.now() - inicio_global
        escribir_log(f"PROCESO COMPLETO FINALIZADO. Tiempo total: {tiempo_total}", consola=True)
        print(f"\n=== FIN DEL PROCESO: {datetime.now().strftime('%H:%M:%S')} ===")
        print(f"Tiempo total de ejecución: {tiempo_total}")

    except Exception as e:
        print(f"\n[FATAL ERROR MAIN] {e}")
        traceback.print_exc()
        escribir_log(f"FATAL MAIN: {e}", consola=True)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
