import os
import time
import shutil
import urllib.parse
import csv
import sys
import re
import threading
import glob 
import pandas as pd
import os
import time

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Creamos un bloqueo para que solo un hilo escriba a la vez
log_lock = threading.Lock()
# --- CONFIGURACIÓN DE RUTAS ---
ruta_usuario_txt = r"C:\ficheros python\usuarioContraseña.txt"
ruta_descarga_txt = r"C:\ficheros python\carpetaChromeDescargas.txt"
ruta_enlaces_txt = r"C:\ficheros python\enlaces.txt"
ruta_empresas_txt = r"C:\ficheros python\Empresas.txt"
ruta_csv_proyectos = r"C:\ficheros python\Proyecto a borrar.csv"
ruta_log = "log_proceso.txt"
ruta_base_bc = r"C:\ArchivosBC"
ruta_excel = os.path.join(ruta_base_bc, "Excel")
ruta_csv_individuales = os.path.join(ruta_base_bc, "CSV")
ruta_errores = os.path.join(ruta_base_bc, "Errores")
dir_base_hilos = r"C:\ficheros python\Temp_Workers"

def inicializar_entorno():
    """Crea la estructura de carpetas y limpia temporales con pausa de seguridad para Windows."""
    # 1. Crear carpetas finales de almacenamiento (Excel, CSV, Errores)
    for carpeta in [ruta_base_bc, ruta_excel, ruta_csv_individuales, ruta_errores]:
        os.makedirs(carpeta, exist_ok=True)
    
    # 2. Limpieza radical de carpetas temporales de trabajo (hilos)
    if os.path.exists(dir_base_hilos):
        try: 
            shutil.rmtree(dir_base_hilos)
            time.sleep(1) # Crucial: Pausa para que Windows libere el índice del disco
        except: 
            pass
    
    # 3. Recrear la base para los hilos
    os.makedirs(dir_base_hilos, exist_ok=True)
    
    escribir_log(f"Entorno preparado. Temporales en: {dir_base_hilos}")
    print(f"[+] Entorno de hilos preparado.")

def procesar_descarga(id_hilo, tarea, empresa, usuario, password, destino_final, filtros_proyectos, max_intentos=3):
    nombre_tarea = f"[{empresa} - {tarea['prefijo']}]"
    clean_emp = limpiar_nombre_archivo(empresa).replace(' ', '_')

    # Espaciado de hilos (anti-BC)
    delay = ((id_hilo - 1) % 3) * 5
    if delay:
        time.sleep(delay)

    for intento in range(1, max_intentos + 1):
        dir_hilo = os.path.join(dir_base_hilos, f"worker_{id_hilo}_{intento}")
        os.makedirs(dir_hilo, exist_ok=True)

        escribir_log(
        f"INTENTO {intento}/{max_intentos} {nombre_tarea} (Hilo {id_hilo})",
        consola=True
        )    

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": dir_hilo,
            "download.prompt_for_download": False,
            "safebrowsing.enabled": True
        })

        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": dir_hilo
        })

        wait = WebDriverWait(driver, 90)

        try:
            if not realizar_login(driver, wait, usuario, password):
                raise Exception("Login fallido")

            filtro_csv = filtros_proyectos.get(
                empresa,
                "%26%3c%3e".join(["*ES000*", "*MODES*", "*CRU*", "*MARGEN*"])
            )

            url_final = (
                tarea['url']
                .replace("empresas.txt", urllib.parse.quote(empresa))
                .replace("Proyecto a borrar.csv", filtro_csv)
            )

            driver.get(url_final)

            wait.until(EC.frame_to_be_available_and_switch_to_it(
                (By.XPATH, "/html/body/div[2]/div[2]/div[1]/div/div[1]/div/iframe")
            ))

            def click_blindado(xpath, nombre):
                for i in range(4):
                    try:
                        el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
                        driver.execute_script(
                            "arguments[0].scrollIntoView({block:'center'});", el
                        )
                        time.sleep(1)
                        el.click()
                        return True
                    except:
                        escribir_log(f"{nombre_tarea}: reintento {nombre} {i+1}/4")
                        time.sleep(3)
                return False

            xpath_menu = "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button"
            xpath_descarga = "/html/body/div[1]/div[2]/form/div/div[2]/div[5]/div/div/div/div[3]/div/div/ul/li/button"

            if not click_blindado(xpath_menu, "MENÚ"):
                raise Exception("No se pudo abrir menú")

            time.sleep(2)

            if not click_blindado(xpath_descarga, "DESCARGA"):
                raise Exception("No se pudo pulsar descarga")

            inicio = time.time()
            archivo = None

            while time.time() - inicio < 3600:
                files = [
                    f for f in os.listdir(dir_hilo)
                    if not f.endswith(('.crdownload', '.tmp'))
                ]
                if files:
                    pos = os.path.join(dir_hilo, files[0])
                    if os.path.getsize(pos) > 500 and archivo_estable(pos):
                        archivo = pos
                        break
                time.sleep(10)

            if not archivo:
                raise Exception("Timeout descarga")

            nuevo = f"{clean_emp}_{tarea['prefijo']}_{datetime.now().strftime('%H%M%S')}"
            ruta_xlsx = os.path.join(ruta_excel, nuevo + ".xlsx")

            if esperar_liberacion_archivo(archivo):
                shutil.move(archivo, ruta_xlsx)

            df = pd.read_excel(ruta_xlsx, engine="openpyxl")
            df.insert(0, "EMPRESA", empresa)

            ruta_csv = os.path.join(ruta_csv_individuales, nuevo + ".csv")
            df.to_csv(ruta_csv, sep=";", index=False, encoding="utf-8-sig")

            escribir_log(f"OK {nombre_tarea} en intento {intento}")
            return {"status": "OK"}

        except Exception as e:
            escribir_log(f"FALLO {nombre_tarea} intento {intento}: {e}")
            try:
                driver.save_screenshot(
                    os.path.join(
                        ruta_errores,
                        f"ERR_{clean_emp}_{intento}_{datetime.now().strftime('%H%M%S')}.png"
                    )
                )
            except:
                pass

            time.sleep(8)

        finally:
            driver.quit()
            shutil.rmtree(dir_hilo, ignore_errors=True)

    return {"status": "ERROR", "empresa": empresa, "tarea": tarea}

def esperar_pagina_cargada(driver, timeout=40):
    try:
        # 1. Espera el DOM estándar
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        
        return True
    except:
        return False

def esperar_liberacion_archivo(ruta, timeout=10):
    for _ in range(int(timeout / 0.5)):
        try:
            with open(ruta, "rb"):
                return True
        except (PermissionError, FileNotFoundError):
            time.sleep(0.5)
    return False

def archivo_estable(ruta, intentos=5, espera=2):
    """
    Versión mejorada: 
    1. Ignora archivos temporales de Chrome.
    2. Verifica que el archivo realmente exista antes de medirlo.
    3. Mayor margen de reintentos.
    """
    # 1. Seguridad: Si es un archivo temporal de Chrome, esperar
    if ruta.endswith('.crdownload') or ruta.endswith('.tmp'):
        return False

    tam_anterior = -1
    
    for i in range(intentos):
        if not os.path.exists(ruta):
            time.sleep(espera)
            continue
            
        try:
            tam_actual = os.path.getsize(ruta)
            
            # Si el tamaño es estable y mayor a 0
            if tam_actual > 0 and tam_actual == tam_anterior:
                with open(ruta, "rb"): 
                    return True
            
            tam_anterior = tam_actual
        except (PermissionError, OSError):
            pass
            
        time.sleep(espera)
    
    return False

def escribir_log(mensaje, consola=False):
    linea = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}"
    with log_lock:
        try:
            with open(ruta_log, "a", encoding="utf-8") as log:
                log.write(linea + "\n")
        except Exception as e:
            print(f"Error escribiendo log: {e}")

    if consola:
        print(linea)
  
              
def limpiar_nombre_archivo(nombre):
    return re.sub(r'[\\/*?:"<>|]', "", nombre)

def realizar_login(driver, wait, usuario, password):
    """Login robusto con detección de carga activa y 3 reintentos."""
    for intento in range(3):
        try:
            driver.get("https://bc.zener.es/ZENER_BC/")
            
            if not esperar_pagina_cargada(driver):
                print(f"      [!] Intento {intento+1}: Login no cargó a tiempo.")
                continue

            # Esperar a que el campo sea interactuable (JS listo)
            user_field = wait.until(EC.element_to_be_clickable((By.ID, "UserName")))
            user_field.clear()
            user_field.send_keys(usuario)
            
            pass_field = driver.find_element(By.ID, "Password")
            pass_field.clear()
            pass_field.send_keys(password)
            
            driver.find_element(By.ID, "submitButton").click()
            
            # Verificación de entrada exitosa
            wait.until(EC.url_contains("ZENER_BC"))
            time.sleep(1)
            return True

        except Exception as e:
            print(f"      [!] Error Login {intento+1}/3: {str(e)[:40]}...")
            if intento < 2:
                time.sleep(3)
                driver.delete_all_cookies()
                driver.refresh()
    return False

def limpiar_directorio_completo(directorio):
    """Borra todo el contenido de la carpeta de destino pero mantiene la carpeta."""
    if not os.path.exists(directorio):
        os.makedirs(directorio)
        return

    print(f"--- LIMPIANDO CARPETA DESTINO: {directorio} ---")
    for filename in os.listdir(directorio):
        file_path = os.path.join(directorio, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path) # Borrar archivo
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path) # Borrar subcarpeta
        except Exception as e:
            print(f"[!] No se pudo borrar {file_path}. Razón: {e}")
    print("--- CARPETA DESTINO LIMPIA ---")

if __name__ == "__main__":
    inicializar_entorno()
    inicio_global = datetime.now()
    escribir_log("=== INICIO DE EJECUCIÓN GLOBAL MULTI-HILO ===")
    print(f"=== INICIANDO MULTI-HILO BC: {inicio_global.strftime('%H:%M:%S')} ===")

    try:
        # Carga de credenciales y configuración
        datos_login = [l.strip() for l in open(ruta_usuario_txt, "r", encoding="utf-8")]
        
        print("[*] Limpiando ejecución anterior...")
        limpiar_directorio_completo(ruta_excel)
        limpiar_directorio_completo(ruta_csv_individuales)

        empresas = [l.strip() for l in open(ruta_empresas_txt, "r", encoding="utf-8") if l.strip()]
        enlaces_raw = [l.strip() for l in open(ruta_enlaces_txt, "r", encoding="utf-8") if l.strip()]

        # --- CARGA DE FILTROS PERSONALIZADOS ---
        filtros_proyectos = {}
        if os.path.exists(ruta_csv_proyectos):
            with open(ruta_csv_proyectos, mode="r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";" if ";" in f.read(10) else ",")
                f.seek(0)
                next(reader) # Saltar cabecera
                for row in reader:
                    emp = row.get("EMPRESA", "").strip()
                    proy = row.get("PROYECTOS A ELIMINAR", "").strip()
                    if emp and proy:
                        filtros_proyectos.setdefault(emp, []).append(proy)
            
            separador = "%26%3c%3e"
            fijos = ["*ES000*", "*MODES*", "*CRU*", "*MARGEN*"]
            for emp in filtros_proyectos:
                filtros_proyectos[emp] = separador.join(filtros_proyectos[emp] + fijos)

        # --- PREPARAR MATRIZ DE TRABAJOS ---
        tareas_base = [{"url": enlaces_raw[i], "prefijo": enlaces_raw[i+1]} 
                       for i in range(0, len(enlaces_raw)-1, 2)]
        
        trabajos = []
        id_base = 1
        for emp in sorted(set(empresas)):
            for tarea in tareas_base:
                trabajos.append((id_base, tarea, emp, datos_login[0], datos_login[1], ruta_base_bc, filtros_proyectos))
                id_base += 1

        # --- EJECUCIÓN POR RONDAS ---
        max_rondas = 3
        ronda = 1
        pendientes = trabajos
        resultados_ok = []

        while pendientes and ronda <= max_rondas:
            escribir_log(f"RONDA {ronda} -> {len(pendientes)} pendientes")
            print(f"\n[*] RONDA {ronda}: Procesando {len(pendientes)} trabajos...")

            with ThreadPoolExecutor(max_workers=3) as executor:
                resultados = list(executor.map(lambda p: procesar_descarga(*p), pendientes))
            
            nuevos_pendientes = []
            for i, r in enumerate(resultados):
                if r.get("status") == "OK":
                    resultados_ok.append(r)
                else:
                    # Preparar para siguiente ronda con ID de hilo único
                    job = list(pendientes[i])
                    job[0] = job[0] + (ronda * 1000)
                    nuevos_pendientes.append(tuple(job))

            print(f"    [√] Fin Ronda {ronda}: {len(resultados) - len(nuevos_pendientes)} OK | {len(nuevos_pendientes)} pendientes.")
            pendientes = nuevos_pendientes
            ronda += 1

        # --- UNIÓN FINAL (CONSOLIDADO) ---
        print("\n" + "-"*40)
        ruta_busqueda = os.path.join(ruta_csv_individuales, "*.csv")
        print("[*] Generando CONSOLIDADO_TOTAL_BC.csv...")
        print(f"[*] Buscando archivos para unir en: {ruta_busqueda}")
        time.sleep(2) # Pausa técnica para Windows I/O

        archivos_csv = glob.glob(os.path.join(ruta_csv_individuales, "*.csv"))
        
        
        if archivos_csv:
            df_maestro = pd.concat([pd.read_csv(f, sep=";", encoding="utf-8-sig") for f in archivos_csv], ignore_index=True)
            ruta_final = os.path.join(ruta_base_bc, "CONSOLIDADO_TOTAL_BC.csv")
            df_maestro.to_csv(ruta_final, index=False, sep=";", encoding="utf-8-sig")
            print(f"[OK] Creado: {ruta_final} ({len(df_maestro)} filas)")
        else:
            print("[!] No hay datos para consolidar.")

        # --- RESUMEN DE ERRORES ---
        if pendientes:
            print("\n[!] TAREAS FALLIDAS TRAS REINTENTOS:")
            for p in pendientes:
                print(f"    - {p[2]} ({p[1]['prefijo']})")

        duracion = str(datetime.now() - inicio_global).split(".")[0]
        resumen = f"DURACIÓN: {duracion} | ÉXITOS: {len(resultados_ok)} | FALLOS FINALIZADOS: {len(pendientes)}"
        print(f"\n{'='*50}\n {resumen}\n{'='*50}")
        escribir_log(f"FIN DE PROCESO: {resumen}")

    except Exception as e:
        print(f"\n[FATAL] Error en el flujo principal: {e}")
        escribir_log(f"ERROR CRÍTICO MAIN: {e}")