# -*- coding: utf-8 -*-
import os
import pandas as pd
from datetime import datetime

# --- CONFIGURACIÓN ---
RUTA_BASE = r"C:\ArchivosBC"
RUTA_MOVS = os.path.join(RUTA_BASE, "Movs. proyectos Unidos.csv")
RUTA_CERT = os.path.join(RUTA_BASE, "Lista Líneas de Certificación Registradas Unidos.csv")
RUTA_RESPONSABLES = r"C:\ficheros python\Responsables.xlsx" # <--- NUEVA RUTA
SALIDA_FINAL = os.path.join(RUTA_BASE, "BC_MAESTRO_FINAL.csv")

# ... (COLUMNAS_FINALES se mantiene igual) ...

def cargar_diccionario_responsables():
    """Lee el Excel de responsables y crea un mapa {Proyecto: Responsable}."""
    if not os.path.exists(RUTA_RESPONSABLES):
        print(f"[!] Aviso: No se encontró {RUTA_RESPONSABLES}. La columna RESPONSABLE irá vacía.")
        return {}
    
    try:
        df_resp = pd.read_excel(RUTA_RESPONSABLES, dtype=str)
        # Limpiamos nombres de columnas y datos para evitar fallos de cruce
        df_resp.columns = df_resp.columns.str.strip().upper()
        # Creamos el diccionario: { 'ES001': 'JUAN PEREZ' }
        # Ajusta "Nº PROYECTO" y "RESPONSABLE" a los nombres reales de tu Excel
        return pd.Series(df_resp.RESPONSABLE.values, index=df_resp["Nº PROYECTO"]).to_dict()
    except Exception as e:
        print(f"[!] Error cargando responsables: {e}")
        return {}

def aplicar_logica_comun(chunk, mapa_resp):
    """Asigna responsables y limpia columnas comunes."""
    # Mapeo de responsable basado en el Nº de proyecto
    # .get() evita errores si el proyecto no existe en el Excel
    chunk["RESPONSABLE"] = chunk["Nº proyecto"].map(mapa_resp).fillna("SIN RESPONSABLE")
    
    # Normalización de la columna DP (asegurar texto y 3 dígitos si fuera necesario)
    if "DP" in chunk.columns:
        chunk["DP"] = chunk["DP"].str.strip()
        
    return chunk

# --- MODIFICACIÓN EN PROCESAR_FUENTE ---

def procesar_fuente(ruta_origen, tipo_fuente, ruta_destino, mapa_resp, modo_escritura='a', escribir_cabecera=False):
    # ... (inicio del lector igual) ...
    for i, chunk in enumerate(lector):
        # Normalizar nombres de columnas del chunk a mayúsculas para evitar líos
        chunk.columns = chunk.columns.str.strip() 

        # 1. Lógica de Negocio
        if tipo_fuente == "MOVIMIENTOS":
            chunk = aplicar_logica_movimientos(chunk)
        else:
            chunk = aplicar_logica_certificaciones(chunk)
        
        # 2. Cruce con Responsables (NUEVO)
        chunk = aplicar_logica_comun(chunk, mapa_resp)
        
        # 3. Formatear y guardar (el resto igual...)
        chunk = asegurar_y_tipar_columnas(chunk)
        # ... (guardado a csv igual) ...

def main():
    # ... (inicio igual) ...
    
    # PASO 0: Cargar responsables en memoria (pesa poco, se puede cargar entero)
    mapa_responsables = cargar_diccionario_responsables()

    # PASO 1: Movimientos (Pasamos el mapa como argumento)
    if os.path.exists(RUTA_MOVS):
        procesar_fuente(RUTA_MOVS, "MOVIMIENTOS", SALIDA_FINAL, mapa_responsables, modo_escritura='w', escribir_cabecera=True)

    # PASO 2: Certificaciones
    if os.path.exists(RUTA_CERT):
        procesar_fuente(RUTA_CERT, "CERTIFICACIONES", SALIDA_FINAL, mapa_responsables, modo_escritura='a', escribir_cabecera=False)