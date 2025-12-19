import pandas as pd
import os
import glob
import time
from datetime import datetime

# --- CONFIGURACIÓN ---
ruta_archivos = r"C:\ArchivosBC" 
ruta_responsables = r"C:\ficheros python\Responsables.xlsx" 
archivo_salida = r"C:\ArchivosBC\Consolidado_BC.csv"

def consolidar():
    inicio_total = time.time()
    print("\n" + "="*60)
    print(f"--- CONSOLIDACIÓN ACTIVA [{datetime.now().strftime('%H:%M:%S')}] ---")
    print("="*60)
    
    # 1. Cargar Maestro
    try:
        df_resp = pd.read_excel(ruta_responsables, sheet_name="DP-RESPONSABLE", engine='openpyxl')
        df_resp.columns = df_resp.columns.str.strip()
        df_resp['COD. DP'] = df_resp['COD. DP'].astype(str).str.strip()
        df_resp = df_resp.rename(columns={"COD. DP": "DP", "NOMBRE ENCARGADO": "RESPONSABLE"})
        print(f"[INFO] Maestro 'Responsables.xlsx' cargado con éxito.")
    except Exception as e:
        print(f"[!] Aviso: No se cargaron responsables ({e})")
        df_resp = pd.DataFrame(columns=["DP", "RESPONSABLE"])

    # 2. Localizar archivos
    archivos = [f for f in glob.glob(os.path.join(ruta_archivos, "*.xlsx")) if not os.path.basename(f).startswith("~$")]
    total = len(archivos)
    print(f"[INFO] Detectados {total} archivos en {ruta_archivos}\n")

    todos_los_df = []
    
    for i, f in enumerate(archivos, 1):
        nombre_f = os.path.basename(f)
        tamano = os.path.getsize(f) / (1024 * 1024)
        
        # Identificar Empresa
        if "_Movs. proyectos" in nombre_f:
            empresa, tipo = nombre_f.split("_Movs. proyectos")[0], "Movimiento"
        elif "_Lista Líneas de Certificación" in nombre_f:
            empresa, tipo = nombre_f.split("_Lista Líneas de Certificación")[0], "Certificacion"
        else: continue

        # --- AVISO DE ACCIÓN ---
        # El end="\r" permite que la línea se actualice, pero aquí usaremos print directo 
        # para que veas el historial de qué empresa ya terminó.
        print(f"[{i}/{total}] Leyendo: {empresa} ({tamano:.1f} MB)...", end=" ", flush=True)
        
        try:
            t_ini = time.time()
            df = pd.read_excel(f, engine='openpyxl')
            t_duracion = time.time() - t_ini
            
            if df.empty:
                print("VACÍO")
                continue

            # --- PROCESO VECTORIZADO ---
            if 'COD. DP' in df.columns: df = df.rename(columns={'COD. DP': 'DP'})
            df['DP'] = df['DP'].astype(str).str.strip()
            df['EMPRESA'] = empresa 

            if tipo == "Movimiento":
                df['PRODUCCIÓN'] = 0.0
                df['FACTURACIÓN'] = -df.get('Importe línea (DL)', 0).fillna(0)
            else:
                df['PRODUCCIÓN'] = df.get('Importe producción actual venta (DL)', 0).fillna(0)
                df['FACTURACIÓN'] = 0.0
                df['PRECIO UNIDAD'] = 0.0
                mask = df.get('Cantidad producción actual', 0) != 0
                df.loc[mask, 'PRECIO UNIDAD'] = df.loc[mask, 'PRODUCCIÓN'] / df.loc[mask, 'Cantidad producción actual']
                df['Tipo movimiento'] = "Producción"

            df['O.C'] = df['PRODUCCIÓN'] - df['FACTURACIÓN']
            
            todos_los_df.append(df)
            print(f"OK ({len(df):,} filas en {t_duracion:.1f}s)")

        except Exception as e:
            print(f"ERROR: {e}")

    # 3. Unión final
    if todos_los_df:
        print(f"\n[*] Uniendo todas las tablas en memoria...", end=" ", flush=True)
        df_final = pd.concat(todos_los_df, ignore_index=True)
        print("Hecho.")
        
        print(f"[*] Cruzando datos con responsables...", end=" ", flush=True)
        df_final = pd.merge(df_final, df_resp[['DP', 'RESPONSABLE']], on='DP', how='left')
        print("Hecho.")
        
        # Reordenar columnas una sola vez
        cols_clave = ["EMPRESA", "DP", "RESPONSABLE", "O.C"]
        otras = [c for c in df_final.columns if c not in cols_clave]
        df_final = df_final[cols_clave + otras]

        print(f"[*] Guardando CSV final...", end=" ", flush=True)
        df_final.to_csv(archivo_salida, index=False, sep=';', encoding='utf-8-sig')
        print("Hecho.")
        
        print("\n" + "="*60)
        print(f"PROCESO TERMINADO EN: {(time.time() - inicio_total)/60:.2f} minutos")
        print(f"FILAS TOTALES CONSOLIDADAS: {len(df_final):,}")
        print("="*60)
    else:
        print("\n[!] No se encontraron datos para consolidar.")

if __name__ == "__main__":
    consolidar()