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

os.makedirs(ruta_errores, exist_ok=True)

# --- FUNCIONES DE SOPORTE ---
def cargar_txt(ruta):
    with open(ruta, "r", encoding="utf-8") as f:
        return [linea.strip() for linea in f.read().splitlines() if linea.strip()]

def limpiar_nombre_archivo(nombre):
    return re.sub(r'[\\/*?:"<>|]', "", nombre)

def escribir_log(mensaje):
    with open(ruta_log, "a", encoding="utf-8") as log:
        log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}\n")

def realizar_login(driver, wait, usuario, password):
    """Maneja el inicio de sesión y verifica que sea exitoso."""
    try:
        driver.get("https://bc.zener.es/ZENER_BC/")
        user_field = wait.until(EC.visibility_of_element_located((By.ID, "UserName")))
        user_field.clear()
        user_field.send_keys(usuario)
        
        pass_field = driver.find_element(By.ID, "Password")
        pass_field.clear()
        pass_field.send_keys(password)
        
        driver.find_element(By.ID, "submitButton").click()
        
        wait.until(EC.url_contains("ZENER_BC"))
        time.sleep(2)
        return True
    except Exception as e:
        escribir_log(f"Error en intento de login: {e}")
        return False

def limpieza_inicial_destino(directorio):
    """Borra todos los archivos de la carpeta de destino al empezar el script."""
    print(f"--- Iniciando limpieza de carpeta de destino: {directorio} ---")
    if os.path.exists(directorio):
        for f in os.listdir(directorio):
            ruta_f = os.path.join(directorio, f)
            try:
                if os.path.isfile(ruta_f):
                    os.remove(ruta_f)
            except Exception as e:
                print(f"No se pudo borrar {f}: {e}")
    else:
        os.makedirs(directorio, exist_ok=True)

def borrar_archivos_empresa_actual(directorio, patron):
    """Borra archivos específicos si una tarea falla a mitad para evitar duplicados/corruptos."""
    for f in os.listdir(directorio):
        if patron in f:
            try:
                os.remove(os.path.join(directorio, f))
                print(f"      [Limpieza] Eliminado archivo previo/incompleto: {f}")
            except: pass

# --- 1. CARGA DE DATOS ---
try:
    datos_login = cargar_txt(ruta_usuario_txt)
    usuario, password = datos_login[0], datos_login[1]
    rutas = cargar_txt(ruta_descarga_txt)
    dir_descargas, dir_destino = rutas[0], rutas[1]
    empresas = cargar_txt(ruta_empresas_txt)
    lineas_enlaces = cargar_txt(ruta_enlaces_txt)
except Exception as e:
    print(f"Error cargando archivos de configuración: {e}")
    sys.exit()

# LIMPIEZA INICIAL DE DESTINO
limpieza_inicial_destino(dir_destino)

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
        # Detectar delimitador automáticamente
        content = f.read(1024)
        f.seek(0)
        delimitador = ';' if ';' in content else ','
        reader = csv.DictReader(f, delimiter=delimitador)
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
prefs = {
    "download.default_directory": dir_descargas,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False 
}
chrome_options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 30)

try:
    if not realizar_login(driver, wait, usuario, password):
        raise Exception("No se pudo iniciar sesión inicial.")

    for emp in empresas:
        emp_url = urllib.parse.quote(emp)
        filtro_csv = filtros_proyectos.get(emp, separador.join(fijos))
        
        for tarea in tareas_base:
            url_final = tarea['url'].replace("empresas.txt", emp_url).replace("Proyecto a borrar.csv", filtro_csv)
            prefix_limpio = limpiar_nombre_archivo(f"{emp}_{tarea['prefijo']}")
            prefix_busqueda = prefix_limpio.replace(' ', '_')
            
            exito_tarea = False
            reintentos_totales = 0

            while reintentos_totales < 3 and not exito_tarea:
                reintentos_totales += 1
                print(f"\n>>> [{emp}] {tarea['prefijo']} | Intento {reintentos_totales}/3")
                
                try:
                    driver.get(url_final)
                    
                    if "signin" in driver.current_url.lower() or "login" in driver.current_url.lower():
                        print("    [!] Sesión perdida. Re-logueando y limpiando rastro...")
                        borrar_archivos_empresa_actual(dir_destino, prefix_busqueda)
                        if not realizar_login(driver, wait, usuario, password):
                            continue
                        driver.get(url_final)

                    # Navegación Iframe y Botones
                    iframe = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[2]/div[1]/div/div[1]/div/iframe")))
                    driver.switch_to.frame(iframe)

                    # Botón 1 (Dinamismo Business Central)
                    xpath_btn1 = "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button"
                    wait.until(EC.element_to_be_clickable((By.XPATH, xpath_btn1))).click()
                    time.sleep(3)

                    # Botón 2 (Ejecutar Descarga)
                    xpath_btn2 = "/html/body/div[1]/div[2]/form/div/div[2]/div[5]/div/div/div/div[3]/div/div/ul/li/button"
                    wait.until(EC.element_to_be_clickable((By.XPATH, xpath_btn2))).click()

                    # Espera de descarga
                    tiempo_inicio = time.time()
                    limite_segundos = 7200 
                    descarga_ok = False

                    while (time.time() - tiempo_inicio) < limite_segundos:
                        archivos_en_temp = [f for f in os.listdir(dir_descargas) if f.startswith(tarea['prefijo'])]
                        descarga_activa = any(f.endswith('.crdownload') for f in os.listdir(dir_descargas))
                        
                        if archivos_en_temp and not descarga_activa:
                            ruta_origen = os.path.join(dir_descargas, archivos_en_temp[0])
                            
                            if os.path.getsize(ruta_origen) < 1024:
                                raise Exception(f"Archivo demasiado pequeño ({os.path.getsize(ruta_origen)} bytes)")
                            
                            time.sleep(2)
                            fecha_str = datetime.now().strftime("%Y%m%d_%H%M")
                            nombre_final = f"{prefix_busqueda}_{fecha_str}{os.path.splitext(archivos_en_temp[0])[1]}"
                            ruta_final_completa = os.path.join(dir_destino, nombre_final)
                            
                            # SOLUCIÓN PUNTO DÉBIL: Reintentos para mover archivo bloqueado
                            for _ in range(5):
                                try:
                                    shutil.move(ruta_origen, ruta_final_completa)
                                    break
                                except PermissionError:
                                    time.sleep(2)

                            print(f"    OK: {nombre_final}")
                            escribir_log(f"{emp} - {tarea['prefijo']} - OK")
                            descarga_ok = True
                            break
                        time.sleep(10)

                    if descarga_ok:
                        exito_tarea = True
                    else:
                        raise Exception("Timeout 2 horas superado")

                except Exception as e:
                    print(f"    Error: {e}")
                    if reintentos_totales == 3:
                        escribir_log(f"{emp} - {tarea['prefijo']} - ERROR: {e}")
                        driver.save_screenshot(os.path.join(ruta_errores, f"ERR_{prefix_busqueda}.png"))
                    driver.switch_to.default_content()
                    time.sleep(5)

finally:
    driver.quit()
    escribir_log("Proceso finalizado.")
    print("\nProceso finalizado.")