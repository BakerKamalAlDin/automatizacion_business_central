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
ruta_log = "log_proceso.txt"

# Carpeta EXCLUSIVA para el bot (Aislamiento total para evitar conflictos)
dir_descargas_bot = r"C:\ficheros python\Descargas_Temporales_Bot"
os.makedirs(dir_descargas_bot, exist_ok=True)
os.makedirs(ruta_errores, exist_ok=True)

def cargar_txt(ruta):
    with open(ruta, "r", encoding="utf-8") as f:
        return [linea.strip() for linea in f.read().splitlines() if linea.strip()]

def limpiar_nombre_archivo(nombre):
    return re.sub(r'[\\/*?:"<>|]', "", nombre)

def escribir_log(mensaje):
    with open(ruta_log, "a", encoding="utf-8") as log:
        log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}\n")

def realizar_login(driver, wait, usuario, password):
    try:
        print(f"[*] Intentando login para: {usuario}")
        driver.get("https://bc.zener.es/ZENER_BC/")
        user_field = wait.until(EC.visibility_of_element_located((By.ID, "UserName")))
        user_field.clear()
        user_field.send_keys(usuario)
        pass_field = driver.find_element(By.ID, "Password")
        pass_field.clear()
        pass_field.send_keys(password)
        driver.find_element(By.ID, "submitButton").click()
        wait.until(EC.url_contains("ZENER_BC"))
        print("[+] Login correcto.")
        return True
    except Exception as e:
        print(f"[!] Error en login: {e}")
        return False

def limpieza_inicial_destino(directorio):
    if os.path.exists(directorio):
        for f in os.listdir(directorio):
            try: os.remove(os.path.join(directorio, f))
            except: pass
    else:
        os.makedirs(directorio, exist_ok=True)

# --- 1. CARGA DE DATOS ---
print("=== INICIANDO PROCESO DE DESCARGAS BC ===")
try:
    datos_login = cargar_txt(ruta_usuario_txt)
    usuario, password = datos_login[0], datos_login[1]
    rutas = cargar_txt(ruta_descarga_txt)
    
    dir_descargas = dir_descargas_bot 
    dir_destino = rutas[1] 
    
    empresas = cargar_txt(ruta_empresas_txt)
    lineas_enlaces = cargar_txt(ruta_enlaces_txt)
    limpieza_inicial_destino(dir_destino)
except Exception as e:
    print(f"[!] Error FATAL en carga de datos: {e}")
    sys.exit()

tareas_base = []
for i in range(0, len(lineas_enlaces), 2):
    if i+1 < len(lineas_enlaces):
        tareas_base.append({"url": lineas_enlaces[i], "prefijo": lineas_enlaces[i+1]})

# --- 2. FILTROS CSV ---
filtros_proyectos = {}
separador = "%26%3c%3e"
fijos = ["*ES000*", "*MODES*", "*CRU*", "*MARGEN*"]
try:
    with open(ruta_csv_proyectos, mode='r', encoding='utf-8-sig') as f:
        content = f.read(1024)
        f.seek(0)
        reader = csv.DictReader(f, delimiter=';' if ';' in content else ',')
        for row in reader:
            emp_n = row.get('EMPRESA', '').strip()
            proy_id = row.get('PROYECTOS A ELIMINAR', '').strip()
            if emp_n and proy_id:
                if emp_n not in filtros_proyectos: filtros_proyectos[emp_n] = []
                filtros_proyectos[emp_n].append(proy_id)
    for emp in filtros_proyectos:
        filtros_proyectos[emp] = separador.join(filtros_proyectos[emp] + fijos)
except: pass

# --- 3. NAVEGADOR ---
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

prefs = {
    "download.default_directory": dir_descargas,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True 
}
chrome_options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=chrome_options)
driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": dir_descargas})
wait = WebDriverWait(driver, 45)

try:
    if not realizar_login(driver, wait, usuario, password): sys.exit()

    for emp in empresas:
        print(f"\n>>> EMPRESA: {emp}")
        emp_url = urllib.parse.quote(emp)
        filtro_csv = filtros_proyectos.get(emp, separador.join(fijos))
        
        for tarea in tareas_base:
            prefix_busqueda = limpiar_nombre_archivo(f"{emp}_{tarea['prefijo']}").replace(' ', '_')
            url_final = tarea['url'].replace("empresas.txt", emp_url).replace("Proyecto a borrar.csv", filtro_csv)
            
            exito_tarea = False
            reintentos = 0
            while reintentos < 3 and not exito_tarea:
                reintentos += 1
                print(f"    - Tarea: {tarea['prefijo']} (Intento {reintentos}/3)")

                # --- LIMPIEZA PERSISTENTE ANTES DE EMPEZAR ---
                archivos_en_descargas = [f for f in os.listdir(dir_descargas) if f.startswith(tarea['prefijo'])]
                while archivos_en_descargas:
                    for f_v in archivos_en_descargas:
                        try: os.remove(os.path.join(dir_descargas, f_v))
                        except OSError: time.sleep(2)
                    archivos_en_descargas = [f for f in os.listdir(dir_descargas) if f.startswith(tarea['prefijo'])]

                try:
                    driver.get(url_final)
                    time.sleep(4)
                    
                    if "signin" in driver.current_url.lower():
                        realizar_login(driver, wait, usuario, password)
                        driver.get(url_final)

                    # Navegación dinámica Business Central
                    iframe = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[2]/div[1]/div/div[1]/div/iframe")))
                    driver.switch_to.frame(iframe)

                    # Botón 1 (Menú)
                    btn1 = wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button")))
                    btn1.click()
                    time.sleep(2)

                    # Botón 2 (Descarga)
                    btn2 = wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[2]/form/div/div[2]/div[5]/div/div/div/div[3]/div/div/ul/li/button")))
                    btn2.click()
                    print("    [*] Descarga solicitada...")

                    # Espera de archivo (máximo 10 min por archivo)
                    inicio_esp = time.time()
                    while (time.time() - inicio_esp) < 7200:
                        files = [f for f in os.listdir(dir_descargas) if f.startswith(tarea['prefijo'])]
                        if files and not any(f.endswith('.crdownload') for f in os.listdir(dir_descargas)):
                            orig = os.path.join(dir_descargas, files[0])
                            if os.path.getsize(orig) > 500: # Tamaño mínimo de seguridad
                                time.sleep(2)
                                final_name = f"{prefix_busqueda}_{datetime.now().strftime('%Y%m%d_%H%M')}{os.path.splitext(files[0])[1]}"
                                ruta_final = os.path.join(dir_destino, final_name)
                                
                                # Evitar duplicados en destino
                                c = 1
                                while os.path.exists(ruta_final):
                                    ext = os.path.splitext(files[0])[1]
                                    ruta_final = os.path.join(dir_destino, f"{prefix_busqueda}_{datetime.now().strftime('%Y%m%d_%H%M')}_{c}{ext}")
                                    c += 1
                                
                                # Mover archivo con reintentos por si Windows lo bloquea
                                for _ in range(5):
                                    try:
                                        shutil.move(orig, ruta_final)
                                        exito_tarea = True
                                        print(f"    [OK] Guardado: {os.path.basename(ruta_final)}")
                                        break
                                    except: time.sleep(2)
                            if exito_tarea: break
                        time.sleep(5)
                        
                except Exception as e:
                    print(f"    [!] Error en intento {reintentos}: {e}")
                    if reintentos == 3:
                        driver.save_screenshot(os.path.join(ruta_errores, f"ERR_{prefix_busqueda}.png"))
                
                driver.switch_to.default_content()

finally:
    driver.quit()
    print("\n=== PROCESO FINALIZADO COMPLETAMENTE ===")
    escribir_log("Ejecución finalizada.")