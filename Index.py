import subprocess
import time
import sys
from datetime import datetime

def ejecutar_script(nombre_script):
    print(f"\n>>> EJECUTANDO: {nombre_script} a las {datetime.now().strftime('%H:%M:%S')}")
    try:
        # Ejecuta el script de Python y espera a que termine
        resultado = subprocess.run([sys.executable, nombre_script], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error crítico en {nombre_script}: {e}")
        return False

if __name__ == "__main__":
    inicio = time.time()
    
    # 1. PASO 1: DESCARGAR (Selenium)
    script_descarga = r"C:\ficheros python\DescargarBC.py"
    if ejecutar_script(script_descarga):
        
        # 2. PASO 2: CONSOLIDAR (Pandas)
        script_union = r"C:\ficheros python\ConsolidarBC.py"
        ejecutar_script(script_union)
        
    fin = time.time()
    total = fin - inicio
    print(f"\n==========================================")
    print(f"PROCESO COMPLETO FINALIZADO EN {round(total/60, 2)} MINUTOS")
    print(f"==========================================")