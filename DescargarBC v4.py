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

from datetime import datetime

from concurrent.futures import ThreadPoolExecutor

from selenium import webdriver

from selenium.webdriver.common.by import By

from selenium.webdriver.chrome.options import Options

from selenium.webdriver.support.ui import WebDriverWait

from selenium.webdriver.support import expected_conditions as EC



# --- CONFIGURACIÓN DE RUTAS ---

ruta_usuario_txt = r"C:\ficheros python\usuarioContraseña.txt"

ruta_descarga_txt = r"C:\ficheros python\carpetaChromeDescargas.txt"

ruta_enlaces_txt = r"C:\ficheros python\enlaces.txt"

ruta_empresas_txt = r"C:\ficheros python\Empresas.txt"

ruta_csv_proyectos = r"C:\ficheros python\Proyecto a borrar.csv"

ruta_log = "log_proceso.txt"



# Rutas Base

ruta_base_bc = r"C:\ArchivosBC"

ruta_excel_base = os.path.join(ruta_base_bc, "Excel")

ruta_csv_base = os.path.join(ruta_base_bc, "CSV")

ruta_errores = os.path.join(ruta_base_bc, "Errores")

dir_base_hilos = r"C:\ficheros python\Temp_Workers"



# Bloqueo para log

log_lock = threading.Lock()



def limpiar_nombre_archivo(nombre):

    """Limpia caracteres prohibidos en nombres de carpeta/archivo de Windows."""

    # Mantenemos espacios y puntos, pero quitamos caracteres ilegales

    limpio = re.sub(r'[\\/*?:"<>|]', "", nombre)

    return limpio.strip()



def inicializar_entorno():

    """Crea la estructura base y limpia temporales."""

    # Creamos solo las raíces, las subcarpetas se crean dinámicamente

    for carpeta in [ruta_base_bc, ruta_excel_base, ruta_csv_base, ruta_errores]:

        os.makedirs(carpeta, exist_ok=True)

    

    # Limpieza de temporales de hilos

    if os.path.exists(dir_base_hilos):

        try: 

            shutil.rmtree(dir_base_hilos)

            time.sleep(1) 

        except: 

            pass

    os.makedirs(dir_base_hilos, exist_ok=True)

    

    escribir_log("Entorno preparado.")



def escribir_log(mensaje, consola=False):

    linea = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}"

    with log_lock:

        try:

            with open(ruta_log, "a", encoding="utf-8") as log:

                log.write(linea + "\n")

        except:

            pass

    if consola:

        print(linea)



def esperar_pagina_cargada(driver, timeout=40):

    try:

        WebDriverWait(driver, timeout).until(

            lambda d: d.execute_script("return document.readyState") == "complete"

        )

        return True

    except:

        return False



def archivo_estable(ruta, intentos=5, espera=2):

    if ruta.endswith('.crdownload') or ruta.endswith('.tmp'):

        return False

    tam_anterior = -1

    for i in range(intentos):

        if not os.path.exists(ruta):

            time.sleep(espera)

            continue

        try:

            tam_actual = os.path.getsize(ruta)

            if tam_actual > 0 and tam_actual == tam_anterior:

                with open(ruta, "rb"): 

                    return True

            tam_anterior = tam_actual

        except:

            pass

        time.sleep(espera)

    return False



def realizar_login(driver, wait, usuario, password):

    for intento in range(3):

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

            return True

        except:

            time.sleep(3)

            driver.delete_all_cookies()

            driver.refresh()

    return False



def procesar_descarga(id_hilo, tarea, empresa, usuario, password, filtros_proyectos, max_intentos=3):

    # Definimos la categoría basada en el prefijo del enlace (ej: "Movs. proyectos")

    categoria_raw = tarea['prefijo']

    categoria_clean = limpiar_nombre_archivo(categoria_raw)

    

    # Rutas específicas para esta categoría

    dir_destino_excel = os.path.join(ruta_excel_base, categoria_clean)

    dir_destino_csv = os.path.join(ruta_csv_base, categoria_clean)

    

    # Aseguramos que existan las carpetas de ESTA categoría

    os.makedirs(dir_destino_excel, exist_ok=True)

    os.makedirs(dir_destino_csv, exist_ok=True)



    nombre_tarea = f"[{empresa} - {categoria_raw}]"

    clean_emp = limpiar_nombre_archivo(empresa).replace(' ', '_')



    # Delay escalonado

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

            if not realizar_login(driver, wait, usuario, password):

                raise Exception("Login fallido")



            filtro_csv = filtros_proyectos.get(empresa, "%26%3c%3e".join(["*ES000*", "*MODES*", "*CRU*", "*MARGEN*"]))

            

            empresa_norm = unicodedata.normalize("NFC", empresa)

            empresa_encoded = urllib.parse.quote(empresa.strip(), safe="", encoding="utf-8")



           

            

            # Construcción URL

            url_final = (

                tarea['url']

                .strip()

                .replace("empresas.txt", empresa_encoded)

                .replace("Proyecto a borrar.csv", filtro_csv)

            )



            driver.get(url_final)

            wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, "/html/body/div[2]/div[2]/div[1]/div/div[1]/div/iframe")))



            # Función Click Blindado (solicitada por usuario)

            def click_blindado(xpath):

                for k in range(4):

                    try:

                        el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))

                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)

                        time.sleep(1)

                        el.click()

                        return True

                    except:

                        time.sleep(3)

                return False



            # XPATHS Fijos (BC Dinámico)

            xpath_menu = "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button"

            xpath_descarga = "/html/body/div[1]/div[2]/form/div/div[2]/div[5]/div/div/div/div[3]/div/div/ul/li/button"



            if not click_blindado(xpath_menu): raise Exception("Error Menú")

            time.sleep(2)

            if not click_blindado(xpath_descarga): raise Exception("Error Botón Descarga")



            # Espera de fichero

            inicio = time.time()

            archivo = None

            while time.time() - inicio < 3600:

                files = [f for f in os.listdir(dir_hilo) if not f.endswith(('.crdownload', '.tmp'))]

                if files:

                    pos = os.path.join(dir_hilo, files[0])

                    if os.path.getsize(pos) > 500 and archivo_estable(pos):

                        archivo = pos

                        break

                time.sleep(10)



            if not archivo: raise Exception("Timeout Descarga")



            # Procesamiento de archivo

            # Nombre incluye categoría para evitar colisiones

            nuevo_nombre = f"{clean_emp}_{categoria_clean}_{datetime.now().strftime('%H%M%S')}"

            ruta_final_excel = os.path.join(dir_destino_excel, nuevo_nombre + ".xlsx")



            shutil.move(archivo, ruta_final_excel)



            # Conversión a CSV

            df = pd.read_excel(ruta_final_excel, engine="openpyxl")

            df.insert(0, "EMPRESA", empresa)

            

            ruta_final_csv = os.path.join(dir_destino_csv, nuevo_nombre + ".csv")

            df.to_csv(ruta_final_csv, sep=";", index=False, encoding="utf-8-sig")



            escribir_log(f"OK {nombre_tarea}")

            return {"status": "OK", "categoria": categoria_clean}



        except Exception as e:

            escribir_log(f"ERROR {nombre_tarea}: {e}")

            try:

                driver.save_screenshot(os.path.join(ruta_errores, f"ERR_{clean_emp}_{intento}.png"))

            except: pass

            time.sleep(5)

        finally:

            driver.quit()

            shutil.rmtree(dir_hilo, ignore_errors=True)



    return {"status": "ERROR", "empresa": empresa, "tarea": tarea}



def limpiar_directorio_recursivo(directorio_base):

    """Borra el contenido de Excel y CSV base antes de empezar."""

    if os.path.exists(directorio_base):

        shutil.rmtree(directorio_base)

    os.makedirs(directorio_base)



def consolidar_archivos_por_categoria():

    """Busca subcarpetas en CSV y crea un consolidado por cada una asegurando todas las columnas."""

    print("\n" + "-"*40)

    print("[*] Iniciando Consolidación por Categoría...")

    

    try:

        subcarpetas = [d for d in os.listdir(ruta_csv_base) if os.path.isdir(os.path.join(ruta_csv_base, d))]

    except FileNotFoundError:

        print("[!] No se encontró carpeta CSV base.")

        return



    if not subcarpetas:

        print("[!] No hay subcarpetas de categorías para consolidar.")

        return



    total_consolidados = 0



    for carpeta_cat in subcarpetas:

        ruta_cat_csv = os.path.join(ruta_csv_base, carpeta_cat)

        archivos_csv = glob.glob(os.path.join(ruta_cat_csv, "*.csv"))

        

        if archivos_csv:

            print(f"    > Procesando '{carpeta_cat}': {len(archivos_csv)} archivos encontrados.")

            try:

                df_list = []

                for f in archivos_csv:

                    # Leemos el CSV asegurando que trate todo como texto para no perder ceros a la izquierda en DP

                    temp_df = pd.read_csv(f, sep=";", encoding="utf-8-sig", dtype=str)

                    # Normalizamos nombres de columnas: quitamos espacios extra (ej: "COD. DP " -> "COD. DP")

                    temp_df.columns = temp_df.columns.str.strip()

                    df_list.append(temp_df)



                # El parámetro sort=False evita que pandas reordene y pierda consistencia

                df_consolidado = pd.concat(df_list, ignore_index=True, sort=False)

                

                nombre_final = f"{carpeta_cat} Unidos.csv"

                ruta_salida = os.path.join(ruta_base_bc, nombre_final)

                

                # Guardamos el maestro

                df_consolidado.to_csv(ruta_salida, index=False, sep=";", encoding="utf-8-sig")

                print(f"      [OK] Generado: {nombre_final} ({len(df_consolidado)} filas y {len(df_consolidado.columns)} columnas)")

                total_consolidados += 1

            except Exception as e:

                print(f"      [ERROR] Fallo al consolidar '{carpeta_cat}': {e}")

        else:

            print(f"    > Omitiendo '{carpeta_cat}': Vacía.")



    print(f"[*] Consolidación finalizada. {total_consolidados} archivos maestros creados.")



if __name__ == "__main__":

    inicializar_entorno()

    inicio_global = datetime.now()

    print(f"=== INICIO PROCESO: {inicio_global.strftime('%H:%M:%S')} ===")



    try:

        # 1. Cargar Credenciales

        with open(ruta_usuario_txt, "r", encoding="utf-8") as f:

            datos_login = [l.strip() for l in f.readlines() if l.strip()]

        

        # 2. Cargar Empresas y Enlaces

        empresas = [l.strip() for l in open(ruta_empresas_txt, "r", encoding="utf-8") if l.strip()]

        enlaces_raw = [l.strip() for l in open(ruta_enlaces_txt, "r", encoding="utf-8") if l.strip()]



        # 3. Limpieza de carpetas

        limpiar_directorio_recursivo(ruta_excel_base)

        limpiar_directorio_recursivo(ruta_csv_base)



        # 4. Carga de filtros (Corregido para evitar saltos de línea)

        filtros_proyectos = {}

        if os.path.exists(ruta_csv_proyectos):

            df_filtros = pd.read_csv(ruta_csv_proyectos, sep=None, engine='python', encoding='utf-8-sig')

            for _, row in df_filtros.iterrows():

                emp = str(row["EMPRESA"]).strip()

                proy = str(row["PROYECTOS A ELIMINAR"]).strip()

                if emp and proy:

                    filtros_proyectos.setdefault(emp, []).append(proy)

        

        sep_url = "%26%3c%3e"

        fijos = ["*ES000*", "*MODES*", "*CRU*", "*MARGEN*"]

        for emp in filtros_proyectos:

            filtros_proyectos[emp] = sep_url.join(filtros_proyectos[emp] + fijos)



        # 5. Preparar Tareas

        tareas_base = []

        for i in range(0, len(enlaces_raw)-1, 2):

            tareas_base.append({

                "url": enlaces_raw[i],

                "prefijo": enlaces_raw[i+1]

            })



        trabajos = []

        id_gen = 1

        for emp in sorted(set(empresas)):

            for tarea in tareas_base:

                trabajos.append((id_gen, tarea, emp, datos_login[0], datos_login[1], filtros_proyectos))

                id_gen += 1



        # 6. Ejecución de Rondas

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



        # 7. Consolidación Final (Donde se asegura la columna DP)

        consolidar_archivos_por_categoria()



        print(f"\n=== FIN === Tiempo total: {datetime.now() - inicio_global}")



    except Exception as e:

        print(f"[FATAL] {e}")

        escribir_log(f"FATAL MAIN: {e}") 