# 🤖 Automatización RPA para Business Central

Solución desarrollada en **Python** para optimizar la extracción de datos y gestión documental en el entorno de **Microsoft Dynamics 365 Business Central**.

## 🚀 Funcionalidades Destacadas
* **Navegación web Dinámica:  Business Central** Superación de retos en el DOM mediante selectores avanzados (XPath) en (iframe) para interactuar con botones y menús dinámicos. (descargas de datos)
* **Post-procesado de Datos: ** Limpieza y estructuración de archivos CSV y Excel mediante scripts especializados. (conversión Excel a csv para unir datos)
* ** Tratamiento de CSV finales en Excel ** Uso de codigo en Power Query y Tratamiento de datos con Power Pivot, indicado en txt


## 🛠️ Stack Tecnológico
* **Lenguaje:** Python 3.x
* **Librerías:** Selenium WebDriver, Pandas.
* **Control de Versiones:** Git (con enfoque en seguridad de credenciales).

* ** Añadidos:** Uso en Excel power Query y Power Pivot

## 📦 Estructura del Proyecto
* `DescargarBC.py`: Script principal de automatización.
* `post_procesado_bc.py`: Lógica de transformación de datos tras la descarga.
* `LANZAR - DescargarBC.bat`: Ejecutor para facilitar el uso al usuario final.


# Automatización de descargas y consolidación Business Central

Script en Python para la **descarga automatizada de datos desde Microsoft Dynamics 365 Business Central**, su **transformación**, **consolidación en CSV** y **actualización automática de un Excel con Power Query**.

---

## 📌 Requisitos

* Python 3.10+
* Google Chrome instalado
* ChromeDriver compatible con la versión de Chrome
* Microsoft Excel (para actualización de Power Query)

### Librerías Python principales

* selenium
* pandas
* numpy
* pywin32
* python-calamine (opcional, recomendado)
* openpyxl

---

## 📂 Archivos y dependencias del proyecto

Todos los archivos se resuelven **relativamente a la ubicación del script** (`.py`). Deben convivir en el mismo directorio base.

---

## 1. Archivos obligatorios

### 🔐 `usuarioContraseña.txt`

Credenciales de acceso a Business Central.

```txt
usuario
contraseña
```

---

### 🏢 `Empresas.txt`

Listado de empresas a procesar (una por línea).

```txt
Empresa 1
Empresa 2
```

---

### 🔗 `enlaces.txt`

Definición de URLs y categorías asociadas (por pares).

```txt
URL_1
Categoria_1
URL_2
Categoria_2
```

> ⚠️ El archivo debe tener un número **par de líneas**.

---

## 2. Archivos opcionales

### 🎯 `Proyecto a borrar.csv`

Filtro de proyectos (Job No.) por empresa.

```csv
Empresa,Proyecto
EMPRESA_1,JOB001
EMPRESA_1,JOB002
```

Si no existe, se descargan todos los proyectos.

---

### 👤 `DP_RESPONSABLE.xlsx`

Tabla maestra para asignar responsables.

Columnas requeridas:

* `COD. DP`
* `NOMBRE ENCARGADO`

---

### 📊 `actualizarExcel.txt`

Ruta a un Excel final con Power Query.

```txt
C:\Ruta\al\informe_final.xlsx
```

Si existe, el script ejecuta `RefreshAll()` automáticamente.

---

## 3. Archivos generados automáticamente

* `log_proceso.txt` → Log general
* `debug_enlaces.txt` → Registro de URLs

Estos archivos se **reinician en cada ejecución**.

---

## 4. Estructura de carpetas generada

```text
ArchivosBC/
├── Excel/
├── CSV/
├── csvProject/
├── Errores/
└── Temp_Workers/
```

No es necesario crear estas carpetas manualmente.

---

## 5. Ejecución

```bash
python DescargarBC.py
```

El proceso se ejecuta en paralelo por empresa y consolida los resultados al finalizar.

---

## 6. Recomendación `.gitignore`

```gitignore
# Logs
log_proceso.txt
debug_enlaces.txt

# Datos generados
ArchivosBC/

# Credenciales
usuarioContraseña.txt
actualizarExcel.txt
```

---

## ✅ Estado

Documentación validada y lista para subida a GitHub.
