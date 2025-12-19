import pandas as pd
import os
import glob
import time
from datetime import datetime

# --- CONFIGURACIÓN ---
ruta_archivos = r"C:\ArchivosBC" 
ruta_responsables = r"C:\ficheros python\Responsables.xlsx" 
salida_movs = r"C:\ArchivosBC\Maestro_Movimientos.csv"
salida_certs = r"C:\ArchivosBC\Maestro_Certificaciones.csv"
salida_final = r"C:\ArchivosBC\Consolidado_Final.csv"

def consolidar_por_fases():
    inicio_total = time.time()
    print("\n" + "="*60)
    print(f"--- FASE 1: EXTRACCIÓN Y UNIFICACIÓN [{datetime.now().strftime('%H:%M:%S')}] ---")
    print("="*60)
    
    archivos = [f for f in glob.glob(os.path.join(ruta_archivos, "*.xlsx")) if not os.path.basename(f).startswith("~$")]
    listado_movs = []
    listado_certs = []

    for i, f in enumerate(archivos, 1):
        nombre_f = os.path.basename(f)
        size_mb = os.path.getsize(f) / (1024 * 1024)
        
        if "_Movs. proyectos" in nombre_f:
            empresa, lista = nombre_f.split("_Movs. proyectos")[0], listado_movs
        elif "_Lista Líneas de Certificación" in nombre_f:
            empresa, lista = nombre_f.split("_Lista Líneas de Certificación")[0], listado_certs
        else: continue

        try:
            print(f"[{i}/{len(archivos)}] {empresa} ({size_mb:.1f} MB)...", end=" ", flush=True)
            t_ini = time.time()
            
            df = pd.read_excel(f, engine='openpyxl')
            
            if not df.empty:
                df['EMPRESA'] = empresa
                lista.append(df)
                print(f"OK ({time.time()-t_ini:.1f}s)")
            else:
                print("VACÍO")
        except Exception as e:
            print(f"ERROR: {e}")

    # Guardado intermedio
    print(f"\n[*] Generando archivos maestros CSV...", end=" ", flush=True)
    if listado_movs: pd.concat(listado_movs).to_csv(salida_movs, index=False, sep=';', encoding='utf-8-sig')
    if listado_certs: pd.concat(listado_certs).to_csv(salida_certs, index=False, sep=';', encoding='utf-8-sig')
    print("Hecho.")

    print("\n" + "="*60)
    print(f"--- FASE 2: PROCESADO MASIVO Y CRUCE ---")
    print("="*60)
    t_fase2 = time.time()

    # 1. Procesar Movimientos
    if os.path.exists(salida_movs):
        print("[*] Procesando Movimientos...", end=" ", flush=True)
        df_m = pd.read_csv(salida_movs, sep=';', encoding='utf-8-sig')
        df_m['PRODUCCIÓN'] = 0.0
        df_m['FACTURACIÓN'] = -df_m.get('Importe línea (DL)', 0).fillna(0)
        df_m = df_m.rename(columns={'COD. DP': 'DP', 'Nº Documento': 'Nº documento', 'No. documento': 'Nº documento'}, errors='ignore')
        print("OK")
    else: df_m = pd.DataFrame()

    # 2. Procesar Certificaciones
    if os.path.exists(salida_certs):
        print("[*] Procesando Certificaciones...", end=" ", flush=True)
        df_c = pd.read_csv(salida_certs, sep=';', encoding='utf-8-sig')
        df_c['PRODUCCIÓN'] = df_c.get('Importe producción actual venta (DL)', 0).fillna(0)
        df_c['FACTURACIÓN'] = 0.0
        # Cálculo vectorizado masivo (mucho más rápido que hacerlo archivo por archivo)
        df_c['PRECIO UNIDAD'] = 0.0
        mask = df_c.get('Cantidad producción actual', 0) != 0
        df_c.loc[mask, 'PRECIO UNIDAD'] = df_c.loc[mask, 'PRODUCCIÓN'] / df_c.loc[mask, 'Cantidad producción actual']
        df_c = df_c.rename(columns={'COD. DP': 'DP'}, errors='ignore')
        print("OK")
    else: df_c = pd.DataFrame()

    # 3. Unir y Cruzar Responsables
    print("[*] Cruce final con Responsables...", end=" ", flush=True)
    df_total = pd.concat([df_m, df_c], ignore_index=True)
    df_total['O.C'] = df_total['PRODUCCIÓN'] - df_total['FACTURACIÓN']
    df_total['DP'] = df_total['DP'].astype(str).str.strip()

    df_resp = pd.read_excel(ruta_responsables, sheet_name="DP-RESPONSABLE")
    df_resp = df_resp.rename(columns={'COD. DP': 'DP', 'NOMBRE ENCARGADO': 'RESPONSABLE'})
    df_resp['DP'] = df_resp['DP'].astype(str).str.strip()
    
    df_final = pd.merge(df_total, df_resp[['DP', 'RESPONSABLE']], on='DP', how='left')
    df_final.to_csv(salida_final, index=False, sep=';', encoding='utf-8-sig')
    print("Hecho.")
    
    print("\n" + "="*60)
    print(f"TIEMPO FASE 1: {(t_fase2 - inicio_total)/60:.2f} min")
    print(f"TIEMPO FASE 2: {(time.time() - t_fase2):.1f} seg")
    print(f"TOTAL: {(time.time() - inicio_total)/60:.2f} min")
    print(f"FILAS CONSOLIDADAS: {len(df_final):,}")
    print("="*60)

if __name__ == "__main__":
    consolidar_por_fases()