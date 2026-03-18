# Manual de Usuario y Guía de Despliegue

## PARTE 1: Configuración Inicial

### 1. Requisitos Previos
*   **Python 3.12+** instalado.
*   **PostgreSQL 16+** instalado y ejecutándose localmente.
*   Una cuenta de **Telegram**.

### 2. Clonar el repositorio y preparar el entorno 

```bash
# 1. Clona el repositorio (o ubícate en la carpeta raíz del proyecto)
git clone <URL_DEL_REPOSITORIO>

# 2. Crea un entorno virtual
python -m venv .venv

# En Windows:
.venv\Scripts\activate

# En macOS/Linux:
source .venv/bin/activate

# 4. Instala las dependencias requeridas
pip install -r requirements.txt
```

## PARTE 2: Obtener el Token de Telegram (BotFather)

1.  Abre la aplicación de Telegram en tu celular o web.
2.  Busca a **@BotFather** .
3.  Inicia un chat y envía el comando `/newbot`.
4.  BotFather te pedirá un **Nombre** para tu bot.
5.  Luego te pedirá un **Username**, el cual debe terminar en `bot`.
6.  BotFather te enviará un mensaje confirmando la creación. En ese mensaje, copia el token generado.

## PARTE 3: Configurar Variables de Entorno

1.  En la carpeta raíz del proyecto (`prueba-tecnica-invers/`), crea un archivo nuevo llamado `.env`.
2.  Abre el archivo y pega la siguiente configuración, reemplazando con tus datos reales:

```env
# Reemplaza 'usuario' y 'password' con tus credenciales locales de postgres.
# 'health' es el nombre de la BD; el script la creará automáticamente si el usuario tiene permisos.
DATABASE_URL=postgresql://usuario:password@localhost:5432/health
TELEGRAM_TOKEN=tu_token_de_telegram
```

## PARTE 4: Ejecución y Validación

1.  En tu terminal (con el entorno virtual activado), ejecuta:
    ```bash
    python src/bot.py
    ```
2.  Si todo está correcto, verás en la consola el mensaje: `Chatbot Online y Escuchando...`

### ¿Cómo validar el funcionamiento desde Telegram?

1.  **Encuentra a tu bot**: En automático de Telegram, escribe el Username que le diste a BotFather.
2.  **Inicia la interacción**: Envía el comando `/start`. Verás un mensaje de bienvenida y botones interactivos.
3.  **VALIDACIÓN**:
    *   Arrastra y suelta el archivo `healthcare_dataset.csv` en el chat del bot.
    *   El bot responderá que ha recibido el documento. En la terminal se disparará la limpieza de datos (`src/02_limpieza.py`) y la carga en base de datos PostgreSQL (`src/03_modelado.py`).
    *   **El resultado**: Una vez insertado en el modelo estrella, el bot te enviará automáticamente un Reporte Ejecutivo con la facturación total y una gráfica generada en tiempo real.
4.  **Menú Analítico**: Toca el botón de "Menú de Reportes" para solicitar gráficas específicas sobre estacionalidad, medicamentos, o aseguradoras, las cuales se generarán consultando la base de datos local.
5.  **Revisión en Base de Datos**: 
    *   Puedes conectarte a tu instancia local de PostgreSQL (PGAdmin) conectándote a la base de datos `health`.
    *   Ahí podrás verificar la creación del Esquema Estrella comprobando que existan las tablas (ej: `fact_admission`, `dim_patient`, `dim_doctor`, etc.).
