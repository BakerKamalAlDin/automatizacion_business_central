import os
import time
import shutil
import urllib.parse
import csv
import sys
import re
from datetime import datetime
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
ruta_errores = r"C:\ficheros python\Errores"
ruta_log = "log_proceso.txt" # Archivo de registro

os.makedirs(ruta_errores, exist_ok=True)

def cargar_txt(ruta):
    with open(ruta, "r", encoding="utf-8") as f:
        return [linea.strip() for linea in f.read().splitlines() if linea.strip()]

def limpiar_nombre_archivo(nombre):
    return re.sub(r'[\\/*?:"<>|]', "", nombre)

def escribir_log(mensaje):
    with open(ruta_log, "a", encoding="utf-8") as log:
        log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}\n")

# --- 1. CARGA DE DATOS ---
datos_login = cargar_txt(ruta_usuario_txt)
usuario, password = datos_login[0], datos_login[1]
rutas = cargar_txt(ruta_descarga_txt)
dir_descargas, dir_destino = rutas[0], rutas[1]
empresas = cargar_txt(ruta_empresas_txt)
lineas_enlaces = cargar_txt(ruta_enlaces_txt)

tareas_base = []
for i in range(0, len(lineas_enlaces), 2):
    if i+1 < len(lineas_enlaces):
        tareas_base.append({"url": lineas_enlaces[i], "prefijo": lineas_enlaces[i+1]})

# --- 2. PROCESAR FILTROS CSV ---
filtros_proyectos = {}
separador = "%26%3c%3e"
fijos = ["*ES000*", "*MODES*", "*CRU*", "*MARGEN*"]

try:
    with open(ruta_csv_proyectos, mode='r', encoding='utf-8-sig') as f:
        primera_linea = f.readline()
        f.seek(0)
        delimitador = ';' if ';' in primera_linea else ','
        reader = csv.DictReader(f, delimiter=delimitador)
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
        
        proyectos_por_empresa = {}
        for row in reader:
            emp_n = row.get('EMPRESA', '').strip()
            proy_id = row.get('PROYECTOS A ELIMINAR', '').strip()
            if emp_n and proy_id:
                if emp_n not in proyectos_por_empresa: proyectos_por_empresa[emp_n] = []
                proyectos_por_empresa[emp_n].append(proy_id)

    for emp, lista_proy in proyectos_por_empresa.items():
        filtros_proyectos[emp] = separador.join(lista_proy + fijos)
except Exception as e:
    print(f"Aviso CSV: {e}")

# --- 3. INICIO DE NAVEGADOR ---
chrome_options = Options()
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-background-networking")

prefs = {
    "download.default_directory": dir_descargas,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False 
}
chrome_options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 30)

errores_consecutivos_globales = 0

try:
    # LOGIN
    driver.get("https://bc.zener.es/ZENER_BC/")
    wait.until(EC.visibility_of_element_located((By.ID, "UserName"))).send_keys(usuario)
    wait.until(EC.visibility_of_element_located((By.ID, "Password"))).send_keys(password)
    wait.until(EC.element_to_be_clickable((By.ID, "submitButton"))).click()
    wait.until(EC.url_contains("ZENER_BC"))
    escribir_log("Sesión iniciada correctamente.")

    for emp in empresas:
        emp_url = urllib.parse.quote(emp)
        filtro_csv = filtros_proyectos.get(emp, separador.join(fijos))
        
        for tarea in tareas_base:
            if errores_consecutivos_globales >= 3:
                escribir_log("ABORTO: 3 fallos críticos consecutivos.")
                sys.exit()

            url_final = tarea['url'].replace("empresas.txt", emp_url).replace("Proyecto a borrar.csv", filtro_csv)
            exito_tarea = False
            reintento_local = 0
            
            prefix_limpio = limpiar_nombre_archivo(f"{emp}_{tarea['prefijo']}")
            prefix_busqueda = prefix_limpio.replace(' ', '_')

            while reintento_local < 3 and not exito_tarea:
                reintento_local += 1
                print(f"\n>>> [{emp}] {tarea['prefijo']} | Intento {reintento_local}/3")

                # Limpieza preventiva
                for f in os.listdir(dir_destino):
                    if prefix_busqueda in f:
                        try: os.remove(os.path.join(dir_destino, f))
                        except: pass
                
                for f in os.listdir(dir_descargas):
                    if f.startswith(tarea['prefijo']):
                        try: os.remove(os.path.join(dir_descargas, f))
                        except: pass

                try:
                    driver.get(url_final)
                    
                    # MEJORA: Validar si la sesión ha caducado
                    if "signin" in driver.current_url.lower():
                        raise Exception("Sesión caducada / Redirección a Login")
                    
                    # Entrar al iframe
                    iframe = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[2]/div[1]/div/div[1]/div/iframe")))
                    driver.switch_to.frame(iframe)

                    # Botón 1 (Navegación dinámica guardada)
                    xpath_btn1 = "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button"
                    wait.until(EC.element_to_be_clickable((By.XPATH, xpath_btn1))).click()
                    time.sleep(3)

                    # Botón 2 (Ejecutar Descarga)
                    xpath_btn2 = "/html/body/div[1]/div[2]/form/div/div[2]/div[5]/div/div/div/div[3]/div/div/ul/li/button"
                    wait.until(EC.element_to_be_clickable((By.XPATH, xpath_btn2))).click()

                    # Espera de hasta 2 horas
                    encontrado = False
                    tiempo_inicio = time.time()
                    limite_segundos = 7200 # 2 horas
                    
                    print("   Esperando descarga...")
                    while (time.time() - tiempo_inicio) < limite_segundos:
                        archivos_en_temp = [f for f in os.listdir(dir_descargas) if f.startswith(tarea['prefijo'])]
                        descarga_activa = any(f.endswith('.crdownload') for f in os.listdir(dir_descargas))
                        
                        if archivos_en_temp and not descarga_activa:
                            ruta_origen = os.path.join(dir_descargas, archivos_en_temp[0])
                            
                            # MEJORA: Validar tamaño mínimo (1024 bytes = 1KB)
                            if os.path.getsize(ruta_origen) < 1024:
                                raise Exception(f"Archivo demasiado pequeño ({os.path.getsize(ruta_origen)} bytes)")
                            
                            time.sleep(2)
                            fecha_str = datetime.now().strftime("%Y%m%d_%H%M")
                            nombre_final = f"{prefix_busqueda}_{fecha_str}{os.path.splitext(archivos_en_temp[0])[1]}"
                            shutil.move(ruta_origen, os.path.join(dir_destino, nombre_final))
                            
                            print(f"    OK: {nombre_final}")
                            escribir_log(f"{emp} - {tarea['prefijo']} - OK")
                            encontrado = True
                            break
                        
                        time.sleep(10)
                    
                    if encontrado:
                        exito_tarea = True
                        errores_consecutivos_globales = 0
                    else:
                        raise Exception("Timeout 2 horas")

                except Exception as e:
                    msg_error = f"{emp} - {tarea['prefijo']} - ERROR Intento {reintento_local}: {e}"
                    print(f"    {msg_error}")
                    if reintento_local == 3:
                        errores_consecutivos_globales += 1
                        escribir_log(msg_error)
                        driver.save_screenshot(os.path.join(ruta_errores, f"ERR_{prefix_busqueda}.png"))
                    else:
                        time.sleep(10)

                finally:
                    driver.switch_to.default_content()

except Exception as e:
    error_critico = f"ERROR CRÍTICO DEL SISTEMA: {e}"
    print(f"\n{error_critico}")
    escribir_log(error_critico)
finally:
    driver.quit()
    escribir_log("Proceso finalizado.")
    print("\nProceso finalizado.")