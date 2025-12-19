# -*- coding: utf-8 -*-
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
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Bloqueo para escritura de logs
log_lock = threading.Lock()

# --- CONFIGURACION DE RUTAS ---
ruta_usuario_txt = r"C:\ficheros python\usuarioContraseña.txt"
ruta_enlaces_txt = r"C:\ficheros python\enlaces.txt"
ruta_empresas_txt = r"C:\ficheros python\Empresas.txt"
ruta_csv_proyectos = r"C:\ficheros python\Proyecto a borrar.csv"
ruta_log = "log_proceso.txt"

ruta_base_bc = r"C:\ArchivosBC"
ruta_excel = os.path.join(ruta_base_bc, "Excel")
ruta_csv_base = os.path.join(ruta_base_bc, "CSV")
ruta_errores = os.path.join(ruta_base_bc, "Errores")

# Subcarpetas especificas solicitadas
dir_movs = os.path.join(ruta_csv_base, "Movs. proyectos")
dir_certif = os.path.join(ruta_csv_base, "Lista Lineas de Certificacion Registradas")
dir_base_hilos = r"C:\ficheros python\Temp_Workers"

def escribir_log(mensaje, consola=False):
    linea = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}"
    with log_lock:
        try:
            with open(ruta_log, "a", encoding="utf-8") as log:
                log.write(linea + "\n")
        except: pass
    if consola: print(linea)

def inicializar_entorno():
    """Crea la estructura de carpetas y limpia temporales."""
    for carpeta in [ruta_base_bc, ruta_excel, ruta_csv_base, ruta_errores, dir_movs, dir_certif]:
        os.makedirs(carpeta, exist_ok=True)
    
    if os.path.exists(dir_base_hilos):
        try: 
            shutil.rmtree(dir_base_hilos)
            time.sleep(1)
        except: pass
    os.makedirs(dir_base_hilos, exist_ok=True)
    escribir_log("Entorno preparado. Carpetas de clasificacion listas.", consola=True)

def archivo_estable(ruta, intentos=5, espera=1):
    if not os.path.exists(ruta) or ruta.endswith(('.crdownload', '.tmp')):
        return False
    tam_anterior = -1
    for _ in range(intentos):
        try:
            tam_actual = os.path.getsize(ruta)
            if tam_actual > 0 and tam_actual == tam_anterior:
                with open(ruta, "rb"): return True
            tam_anterior = tam_actual
        except: pass
        time.sleep(espera)
    return False

def realizar_login(driver, wait, usuario, password):
    try:
        driver.get("https://bc.zener.es/ZENER_BC/")
        user_field = wait.until(EC.element_to_be_clickable((By.ID, "UserName")))
        user_field.send_keys(usuario)
        pass_field = driver.find_element(By.ID, "Password")
        pass_field.send_keys(password)
        driver.find_element(By.ID, "submitButton").click()
        wait.until(EC.url_contains("ZENER_BC"))
        return True
    except: return False

def procesar_descarga(id_hilo, tarea, empresa, usuario, password, filtros_proyectos):
    prefijo = tarea['prefijo']
    nombre_tarea = f"[{empresa} - {prefijo}]"
    clean_emp = re.sub(r'[\\/*?:"<>|]', "", empresa).replace(' ', '_')
    
    # Delay inteligente anti-bloqueo para 5 hilos
    time.sleep((id_hilo % 5) * 2)

    for intento in range(1, 3):
        dir_hilo = os.path.join(dir_base_hilos, f"worker_{id_hilo}_{intento}")
        os.makedirs(dir_hilo, exist_ok=True)
        
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": dir_hilo,
            "download.prompt_for_download": False
        })
        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": dir_hilo})
        wait = WebDriverWait(driver, 60)

        try:
            if not realizar_login(driver, wait, usuario, password): raise Exception("Login fallido")

            filtro_csv = filtros_proyectos.get(empresa, "%26%3c%3e".join(["*ES000*", "*MODES*", "*CRU*", "*MARGEN*"]))
            url_final = tarea['url'].replace("empresas.txt", urllib.parse.quote(empresa)).replace("Proyecto a borrar.csv", filtro_csv)
            
            driver.get(url_final)
            wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, "/html/body/div[2]/div[2]/div[1]/div/div[1]/div/iframe")))

            # Navegacion segun XPATH de Business Central
            xpath_menu = "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button"
            btn_menu = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_menu)))
            driver.execute_script("arguments[0].click();", btn_menu)
            
            time.sleep(2)
            xpath_descarga = "//button[contains(., 'Descargar') or contains(., 'Excel')]"
            btn_down = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_descarga)))
            btn_down.click()

            # Espera de archivo optimizada
            archivo = None
            inicio_espera = time.time()
            while time.time() - inicio_espera < 120:
                files = [f for f in os.listdir(dir_hilo) if not f.endswith(('.crdownload', '.tmp'))]
                if files:
                    pos = os.path.join(dir_hilo, files[0])
                    if archivo_estable(pos):
                        archivo = pos
                        break
                time.sleep(2)

            if not archivo: raise Exception("Timeout descarga")

            # Movimiento a carpeta temporal Excel
            timestamp = datetime.now().strftime('%H%M%S')
            nombre_final = f"{clean_emp}_{prefijo}_{timestamp}"
            ruta_xlsx = os.path.join(ruta_excel, nombre_final + ".xlsx")
            shutil.move(archivo, ruta_xlsx)

            # Clasificacion por tipo de prefijo
            df = pd.read_excel(ruta_xlsx, engine="openpyxl")
            df.insert(0, "EMPRESA", empresa)
            
            # Decidir carpeta segun el contenido del prefijo
            if "Movs. proyectos" in prefijo:
                dest_csv = dir_movs
            else:
                dest_csv = dir_certif

            ruta_csv = os.path.join(dest_csv, nombre_final + ".csv")
            df.to_csv(ruta_csv, sep=";", index=False, encoding="utf-8-sig")

            escribir_log(f"OK: {nombre_tarea}")
            return {"status": "OK"}

        except Exception as e:
            escribir_log(f"Error {nombre_tarea}: {e}")
        finally:
            driver.quit()
            shutil.rmtree(dir_hilo, ignore_errors=True)
    return {"status": "ERROR"}

def limpiar_directorio_completo(directorio):
    if not os.path.exists(directorio): return
    for filename in os.listdir(directorio):
        file_path = os.path.join(directorio, filename)
        try:
            if os.path.isfile(file_path): os.unlink(file_path)
            elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except: pass

if __name__ == "__main__":
    inicializar_entorno()
    inicio_global = datetime.now()
    
    try:
        login = [l.strip() for l in open(ruta_usuario_txt, "r", encoding="utf-8")]
        empresas = [l.strip() for l in open(ruta_empresas_txt, "r", encoding="utf-8") if l.strip()]
        enlaces = [l.strip() for l in open(ruta_enlaces_txt, "r", encoding="utf-8") if l.strip()]
        
        limpiar_directorio_completo(ruta_excel)
        limpiar_directorio_completo(dir_movs)
        limpiar_directorio_completo(dir_certif)
    except Exception as e:
        print(f"Error inicial: {e}"); sys.exit(1)

    # Carga de filtros personalizados
    filtros = {}
    if os.path.exists(ruta_csv_proyectos):
        with open(ruta_csv_proyectos, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                emp = row.get("EMPRESA", "").strip()
                proy = row.get("PROYECTOS A ELIMINAR", "").strip()
                if emp and proy: filtros.setdefault(emp, []).append(proy)
        for emp in filtros: filtros[emp] = "%26%3c%3e".join(filtros[emp] + ["*ES000*", "*MODES*"])

    # Preparar matriz de trabajos
    tareas_base = [{"url": enlaces[i], "prefijo": enlaces[i+1]} for i in range(0, len(enlaces)-1, 2)]
    trabajos = []
    id_b = 1
    for emp in sorted(set(empresas)):
        for t in tareas_base:
            trabajos.append((id_b, t, emp, login[0], login[1], filtros))
            id_b += 1

    # Ejecucion con 5 hilos para optimizar tiempo
    pendientes = trabajos
    resultados_ok = []
    for ronda in range(1, 3):
        if not pendientes: break
        print(f"\n[*] RONDA {ronda}: Procesando {len(pendientes)} tareas con 5 hilos...")
        with ThreadPoolExecutor(max_workers=5) as executor:
            resultados = list(executor.map(lambda p: procesar_descarga(*p), pendientes))
        
        nuevos_p = []
        for i, r in enumerate(resultados):
            if r.get("status") == "OK": resultados_ok.append(r)
            else: nuevos_p.append(pendientes[i])
        pendientes = nuevos_p

    # Consolidacion final en dos archivos separados en C:\ArchivosBC
    print("\n" + "-"*40)
    config_union = [
        ("Movs. Proyectos", dir_movs, "MOV_PROYECTOS"),
        ("Certificaciones Registradas", dir_certif, "LISTA_CERTIF")
    ]

    for nombre_tipo, ruta_carpeta, sufijo in config_union:
        archivos = glob.glob(os.path.join(ruta_carpeta, "*.csv"))
        if archivos:
            print(f"[*] Uniendo {nombre_tipo}...")
            df_lista = []
            for f in archivos:
                df_lista.append(pd.read_csv(f, sep=";", encoding="utf-8-sig", dtype=str, low_memory=False))
            
            df_final = pd.concat(df_lista, ignore_index=True, sort=False)
            ruta_final = os.path.join(ruta_base_bc, f"CONSOLIDADO_{sufijo}.csv")
            df_final.to_csv(ruta_final, index=False, sep=";", encoding="utf-8-sig")
            print(f"[OK] Generado: CONSOLIDADO_{sufijo}.csv")
        else:
            print(f"[!] No se encontraron archivos para {nombre_tipo}")

    duracion = str(datetime.now() - inicio_global).split(".")[0]
    print(f"\n{'='*50}\nTOTAL FINALIZADO: {duracion} | EXITOS: {len(resultados_ok)}\n{'='*50}")