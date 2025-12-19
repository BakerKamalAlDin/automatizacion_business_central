import os
import time
import shutil
import urllib.parse
import csv
import sys
import re
import threading
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
ruta_errores = r"C:\ficheros python\Errores"
ruta_log = "log_proceso.txt"

# Carpeta base para los hilos (se crearán subcarpetas d_1, d_2, etc.)
dir_base_hilos = r"C:\ficheros python\Temp_Workers"

# Crear directorios base si no existen
os.makedirs(ruta_errores, exist_ok=True)
if os.path.exists(dir_base_hilos):
    try: shutil.rmtree(dir_base_hilos) # Limpieza inicial radical
    except: pass
os.makedirs(dir_base_hilos, exist_ok=True)

# --- FUNCIONES AUXILIARES ---

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

def escribir_log(mensaje):
    with log_lock: # Solo un hilo entra aquí, los demás esperan su turno
        try:
            with open(ruta_log, "a", encoding="utf-8") as log:
                log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}\n")
        except Exception as e:
            print(f"Error escribiendo en log: {e}")   
        
        

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

# --- LÓGICA DEL WORKER (HILO) ---


#def procesar_descarga

def procesar_descarga(id_hilo, tarea, empresa, usuario, password, destino_final, filtros_proyectos):
    """
    BLOQUE COMPLETO: Navegación blindada + Gestión de archivos (WinError 32)
    """
    inicio_hilo = datetime.now()
    
    espera_inicial = ((id_hilo - 1) % 3) * 5
    if espera_inicial > 0:
        time.sleep(espera_inicial)
    
      
    # 1. Crear entorno aislado por hilo
    dir_hilo = os.path.join(dir_base_hilos, f"worker_{id_hilo}")
    os.makedirs(dir_hilo, exist_ok=True)
    
    # 2. Configuración de Chrome (Optimizado para estabilidad)
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    prefs = {
        "download.default_directory": dir_hilo,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": dir_hilo})
    
    wait = WebDriverWait(driver, 60) 
    nombre_tarea = f"[{empresa} - {tarea['prefijo']}]"
    print(f"[*] Hilo {id_hilo}: Iniciando {nombre_tarea} a las {inicio_hilo.strftime('%H:%M:%S')}")

    try:
        # --- LOGIN ---
        if not realizar_login(driver, wait, usuario, password):
            raise Exception("Fallo en Login")

        # --- PREPARACIÓN DE URL Y FILTROS ---
        separador = "%26%3c%3e"
        fijos = ["*ES000*", "*MODES*", "*CRU*", "*MARGEN*"]
        filtro_csv = filtros_proyectos.get(empresa, separador.join(fijos))
        emp_url = urllib.parse.quote(empresa)
        url_final = tarea['url'].replace("empresas.txt", emp_url).replace("Proyecto a borrar.csv", filtro_csv)
        
        driver.get(url_final)
        
        # --- ENTRAR EN EL IFRAME DE BC ---
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, "/html/body/div[2]/div[2]/div[1]/div/div[1]/div/iframe")))

        # --- FUNCIÓN INTERNA DE CLICK REFORZADO ---
        def click_blindado(xpath, nombre_paso):
            for i in range(4): # 4 intentos
                try:
                    el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
                    # Scroll y foco para asegurar que el click llegue
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
                    time.sleep(1) 
                    el.click()
                    return True
                except:
                    print(f"      [!] Hilo {id_hilo}: Reintentando {nombre_paso} ({i+1}/4)...")
                    time.sleep(3)
            return False

        # --- PASOS DE NAVEGACIÓN ---
        # 1. Menú (el botón dinámico que guardamos)
        xpath_menu = "/html/body/div[1]/div[2]/form/div/div[2]/div[2]/div/div/nav/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/div/div[2]/div[1]/button"
        if not click_blindado(xpath_menu, "Menú"):
            raise Exception("No se pudo abrir el Menú tras 4 intentos")
        
        time.sleep(2)

        # 2. Botón Descarga
        xpath_descarga = "/html/body/div[1]/div[2]/form/div/div[2]/div[5]/div/div/div/div[3]/div/div/ul/li/button"
        if not click_blindado(xpath_descarga, "Descargar"):
             raise Exception("No se pudo pulsar Descargar")
        
        # --- GESTIÓN DE LA DESCARGA (ESPERA DE ARCHIVO) ---
        inicio_wait = time.time()
        archivo_encontrado = None
        
        # Esperar hasta 10 minutos (600s) a que aparezca el archivo
        while (time.time() - inicio_wait) < 600:
            # Listar archivos excluyendo temporales de Chrome
            files = [f for f in os.listdir(dir_hilo) 
                     if not f.endswith(('.crdownload', '.tmp', '.htm')) 
                     and not f.lower().startswith('download')]
            
            if files:
                posible_archivo = os.path.join(dir_hilo, files[0])
                # Verificar que el archivo tenga contenido y no esté creciendo (estable)
                if os.path.getsize(posible_archivo) > 500 and archivo_estable(posible_archivo):
                    archivo_encontrado = posible_archivo
                    break
            time.sleep(3)
            
        if archivo_encontrado:
            # --- RENOMBRADO Y MOVIMIENTO FINAL ---
            clean_emp = limpiar_nombre_archivo(empresa).replace(' ', '_')
            clean_pref = limpiar_nombre_archivo(tarea['prefijo'])
            extension = os.path.splitext(archivo_encontrado)[1]
            nuevo_nombre = f"{clean_emp}_{clean_pref}_{datetime.now().strftime('%Y%m%d_%H%M')}{extension}"
            ruta_destino = os.path.join(destino_final, nuevo_nombre)
            
            # Evitar duplicados si el archivo ya existe
            c = 1
            while os.path.exists(ruta_destino):
                base, ext = os.path.splitext(nuevo_nombre)
                ruta_destino = os.path.join(destino_final, f"{base}_{c}{ext}")
                c += 1

            # Movimiento seguro contra WinError 32 (5 intentos)
            movido = False
            for _ in range(5):
                try:
                    if esperar_liberacion_archivo(archivo_encontrado):
                        shutil.move(archivo_encontrado, ruta_destino)
                        movido = True
                        break
                except PermissionError:
                    time.sleep(2)
            
            if movido:
                duracion = datetime.now() - inicio_hilo
                print(f"    [OK] Hilo {id_hilo}: {os.path.basename(ruta_destino)} (Tardó: {str(duracion).split('.')[0]})")
                escribir_log(f"OK: {nombre_tarea} - Duración: {duracion}")
                return True
        
        raise Exception("Timeout o error al recibir el archivo de Excel")

    except Exception as e:
        # --- GESTIÓN DE ERRORES ---
        print(f"    [ERROR] Hilo {id_hilo}: {e}")
        escribir_log(f"ERROR en {nombre_tarea}: {e}")
        try: 
            # Guardar captura de pantalla para debug
            driver.save_screenshot(os.path.join(ruta_errores, f"ERR_{id_hilo}_{limpiar_nombre_archivo(empresa)[:10]}.png"))
        except: 
            pass
        return False
        
    finally:
        # --- CIERRE Y LIMPIEZA ---
        driver.quit()
        try:
            shutil.rmtree(dir_hilo, ignore_errors=True)
        except:
            pass


# --- FUNCIÓN DE LIMPIEZA DE DESTINO ---
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

# --- MAIN ---
def inicializar_entorno():
    """Crea y limpia la base de carpetas temporales para los hilos."""
    if os.path.exists(dir_base_hilos):
        try:
            shutil.rmtree(dir_base_hilos)
            time.sleep(1) # Pausa para que Windows confirme el borrado
        except:
            pass
    os.makedirs(dir_base_hilos, exist_ok=True)
    os.makedirs(ruta_errores, exist_ok=True)
    print(f"[+] Entorno de hilos preparado en: {dir_base_hilos}")


if __name__ == "__main__":
    inicializar_entorno() # Limpia carpetas temporales de los hilos
    
    inicio_global = datetime.now()
    print(f"=== INICIANDO MULTI-HILO BC: {inicio_global.strftime('%H:%M:%S')} ===")

    # 1. Carga de Datos Estáticos
    try:
        datos_login = [linea.strip() for linea in open(ruta_usuario_txt, "r", encoding="utf-8")]
        rutas_desc = [linea.strip() for linea in open(ruta_descarga_txt, "r", encoding="utf-8")]
        
        # Definir destino final
        destino_final = rutas_desc[1]

        # ### NUEVO: LIMPIEZA DE CARPETA DESTINO ###
        # Esto borrará los Excel viejos antes de empezar nada
        limpiar_directorio_completo(destino_final)
        # ##########################################
        
        empresas = [l.strip() for l in open(ruta_empresas_txt, "r", encoding="utf-8") if l.strip()]
        enlaces_raw = [l.strip() for l in open(ruta_enlaces_txt, "r", encoding="utf-8") if l.strip()]
        
    except Exception as e:
        print(f"FATAL: Error cargando archivos TXT: {e}")
        sys.exit()

    # 2. Carga de Filtros CSV
    print("--- Cargando Filtros CSV ---")
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
        # Unir filtros
        for emp in filtros_proyectos:
            filtros_proyectos[emp] = separador.join(filtros_proyectos[emp] + fijos)
    except Exception as e:
        print(f"[!] Alerta: No se pudo leer el CSV correctamente o está vacío: {e}")

    # 3. Preparar Lista de Trabajos
    tareas_base = []
    for i in range(0, len(enlaces_raw), 2):
        if i+1 < len(enlaces_raw):
            tareas_base.append({"url": enlaces_raw[i], "prefijo": enlaces_raw[i+1]})

    trabajos = []
    contador_id = 1
    
    # Matriz: Empresas x Tareas
    for emp in empresas:
        for tarea in tareas_base:
            args = (
                contador_id,
                tarea,
                emp,
                datos_login[0], 
                datos_login[1], 
                destino_final,
                filtros_proyectos
            )
            trabajos.append(args)
            contador_id += 1

    print(f"[*] Total de informes a procesar: {len(trabajos)}")
    print(f"[*] Ejecutando con 3 Navegadores simultáneos...")

    # 4. EJECUCIÓN PARALELA (Usar max_workers=3 para estabilidad)
   # 4. EJECUCIÓN PARALELA
    with ThreadPoolExecutor(max_workers=3) as executor:
        # Convertimos a lista para poder contar aciertos/fallos
        resultados = list(executor.map(lambda p: procesar_descarga(*p), trabajos))

    # --- RESUMEN FINAL ---
    exitos = resultados.count(True)
    fallos = resultados.count(False)
    duracion = datetime.now() - inicio_global
    
    print("\n" + "="*40)
    print(f" FINALIZADO EN: {str(duracion).split('.')[0]}")
    print(f" [+] Descargas OK: {exitos}")
    print(f" [!] Errores:      {fallos}")
    print("="*40)
    
    if fallos > 0:
        print(f" Revisa las capturas en: {ruta_errores}")