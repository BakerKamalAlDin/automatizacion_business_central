# -*- coding: utf-8 -*-
import os
from datetime import datetime

# --- CONFIGURACIÓN DE RUTAS PARA LA PRUEBA ---
ruta_usuario_sp_txt = r"C:\ficheros python\usuarioContraseñaSharePoint.txt"
ruta_log = "log_prueba_sharepoint.txt"

def escribir_log(mensaje, consola=False):
    linea = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {mensaje}"
    try:
        with open(ruta_log, "a", encoding="utf-8") as log:
            log.write(linea + "\n")
    except: pass
    if consola: print(linea)

def subir_a_sharepoint_zener(ruta_local_archivo):
    """Sube un archivo de prueba a SharePoint."""
    try:
        from office365.sharepoint.client_context import ClientContext
        from office365.runtime.auth.user_credential import UserCredential
    except ImportError:
        escribir_log("ERROR: Falta la librería 'Office365-REST-Python-Client'. Ejecuta: pip install Office365-REST-Python-Client", consola=True)
        return

    site_url = "https://zenerorg.sharepoint.com/sites/g365_facturacion"
    
    try:
        if not os.path.exists(ruta_usuario_sp_txt):
            raise Exception(f"No existe el archivo de credenciales en: {ruta_usuario_sp_txt}")

        with open(ruta_usuario_sp_txt, "r", encoding="utf-8") as f:
            datos = [l.strip() for l in f.readlines() if l.strip()]
            if len(datos) < 2:
                raise Exception("El archivo txt debe tener: línea 1 email, línea 2 contraseña.")
            user_email = datos[0]
            password = datos[1]

        escribir_log(f"Intentando conectar como: {user_email}...", consola=True)
        ctx = ClientContext(site_url).with_credentials(UserCredential(user_email, password))
        
        # Intentar acceder a la carpeta
        target_folder_url = "/sites/g365_facturacion/Shared Documents/Projects/datos"
        target_folder = ctx.web.get_folder_by_server_relative_url(target_folder_url)
        ctx.load(target_folder)
        ctx.execute_query()
        
        # Subida
        size = os.path.getsize(ruta_local_archivo)
        file_name = os.path.basename(ruta_local_archivo)
        print(f"[*] Subiendo: {file_name} ({round(size/1024, 2)} KB)...")

        chunk_size = 1024 * 1024 # 1MB para la prueba
        target_folder.files.create_upload_session(ruta_local_archivo, chunk_size, lambda offset: print(f" > {offset} bytes")).execute_query()
            
        escribir_log("PRUEBA EXITOSA: Archivo en SharePoint.", consola=True)

    except Exception as e:
        error_msg = f"ERROR EN PRUEBA: {str(e)}"
        escribir_log(error_msg, consola=True)

if __name__ == "__main__":
    # 1. Creamos un archivo de texto pequeño para la prueba
    archivo_test = "test_conexion.txt"
    with open(archivo_test, "w") as f:
        f.write("Prueba de conexion SharePoint Zener " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    print("=== INICIANDO TEST DE SHAREPOINT ===")
    subir_a_sharepoint_zener(os.path.abspath(archivo_test))