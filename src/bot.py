import os
import asyncio
import pandas as pd
import plotly.express as px
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from sqlalchemy import create_engine
from dotenv import load_dotenv

from database import DatabaseManager

# Cargar variables de entorno
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Configuración
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# Inicializar Manejador de Base de Datos
db_manager = DatabaseManager(DATABASE_URL)

# Importar pipeline
import importlib
pipeline = importlib.import_module("04_run_pipeline")


# --- MOTOR DE REPORTES ---

async def send_automated_executive_report(update: Update):
    """Genera un reporte completo KPIs + Visualización tras la ingesta."""
    try:
        # 1. Obtener KPIs Globales usando el manager
        kpis = db_manager.get_executive_kpis()
        
        # 2. Construir Texto
        report_msg = [
            "📊 **REPORTE EJECUTIVO PROACTIVO**",
            "--- Carga Exitosa ---\n",
            f"✅ **Total de Registros en BD:** {int(kpis['total_records']):,}",
            f"💰 **Facturacion Total:** ${kpis['total_billing']/1e6:.2f}M",
            f"⚠️ **Resultados Anormales:** {kpis['abnormal_rate']:.1f}%",
            f"🕒 **Estancia Promedio:** {kpis['avg_stay']:.1f} días",
            "\n--------------------------"
        ]
        
        kpi_text = "\n".join(report_msg)
        
        # 3. Generar Gráfico usando el manager
        df = db_manager.get_top_hospitals_revenue()
        fig = px.bar(df, x='hospital_name', y='total', title='Estado de Red: Top 5 Hospitales', color='total', template='plotly_dark')
        
        img_path = "auto_report_summary.png"
        fig.write_image(img_path)
        
        # 4. Enviar todo
        await update.message.reply_photo(
            photo=open(img_path, 'rb'), 
            caption=kpi_text,
            parse_mode='Markdown'
        )
        os.remove(img_path)
        
    except Exception as e:
        await update.message.reply_text(f"⚠️ Reporte automático falló: {str(e)}")

async def generate_specific_report(update_source, report_type):
    """Genera reportes específicos del menú bajo demanda."""
    message = update_source.message if hasattr(update_source, 'message') else update_source
    
    # --- VALIDACIÓN DE DATOS ---
    if not db_manager.check_data_exists():
        await message.reply_text(
            "⚠️ **No hay datos disponibles.**\n\n"
            "Por favor, envía un archivo **CSV** primero para procesar la información antes de generar reportes."
        )
        return

    try:
        df = db_manager.get_report_data(report_type)
        
        if report_type == 'q1_seasonality':
            df['Periodo'] = df['year'].astype(str) + "-" + df['month'].astype(str).str.zfill(2)
            fig = px.line(df, x='Periodo', y='admisiones', markers=True, title="Q1: Tendencia de Admisiones", template='plotly_white')
            caption = "📈 **Análisis de Estacionalidad:** Se observa el flujo histórico de ingresos."
            
        elif report_type == 'q2_meds':
            fig = px.bar(df, x='total', y='medication_name', orientation='h', title="Q2: Top 10 Medicamentos (Revenue)", color='total')
            caption = "💊 **Impacto de Medicamentos:** Distribución por facturación generada."

        elif report_type == 'q5_insurance':
            fig = px.pie(df, values='total', names='provider_name', title="Q5: Market Share por Aseguradora")
            caption = "🛡️ **Distribución por Seguros:** Concentración de ingresos por proveedor."

        elif report_type == 'q_doctors':
            fig = px.scatter(df, x='casos', y='total', size='total', text='doctor_name', title="Rendimiento Médico: Casos vs Facturación")
            caption = "👨‍⚕️ **Desempeño Médico:** Médicos con mayor volumen y facturación asociada."

        img_path = f"manual_{report_type}.png"
        fig.write_image(img_path)
        await message.reply_photo(photo=open(img_path, 'rb'), caption=caption, parse_mode='Markdown')
        os.remove(img_path)
        
    except Exception as e:
        await message.reply_text(f"⚠️ Error al generar reporte: {str(e)}")

# --- HANDLERS ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manda el mensaje inicial con botones."""
    keyboard = [
        [InlineKeyboardButton("📊 Menú de Reportes", callback_data='menu_reports')],
        [InlineKeyboardButton("📁 Enviar CSV", callback_data='csv_info')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = (
        "🧠 **HEALTH DATA CHATBOT**\n\n"
        "Soy tu asistente virtual. Puedo procesar tus cargas y generar insights estratégicos.\n\n"
        "**¿Qué quieres hacer hoy?**"
    )
    
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def show_reports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra el menú de reportes (soporta tanto mensaje de texto como botón)."""
    keyboard = [
        [InlineKeyboardButton("📈 Estacionalidad (Q1)", callback_data='q1_seasonality')],
        [InlineKeyboardButton("💊 Top Medicamentos (Q2)", callback_data='q2_meds')],
        [InlineKeyboardButton("👨‍⚕️ Desempeño Médico (BI)", callback_data='q_doctors')],
        [InlineKeyboardButton("🛡️ Seguros (Q5)", callback_data='q5_insurance')],
        [InlineKeyboardButton("🔙 Regresar", callback_data='main_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "🔍 **PANEL ANALÍTICO ESTRATÉGICO:**"

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Capa conversacional para responder a interacciones humanas básicas."""
    user_text = update.message.text.lower()
    
    if any(greet in user_text for greet in ["hola", "buen", "saludos", "hi"]):
        reply = (
            f"¡Hola {update.effective_user.first_name}! 👋\n\n"
            "¡Es un gusto saludarte! Soy tu **Asistente de Inteligencia Hospitalaria**. 🏥✨\n\n"
            "Estos son mis **comandos principales**:\n\n"
            "🔹 /start - Abre el menú principal con todas las opciones.\n"
            "🔹 **Enviar un CSV** - Sincroniza y limpia tus datos automáticamente.\n"
            "🔹 **'Reporte'** - Accede directamente al panel de gráficas.\n"
            "¿En qué puedo apoyarte hoy?"
        )
        await update.message.reply_text(reply, parse_mode='Markdown')
    
    elif any(word in user_text for word in ["ayuda", "como","menu"]):
        await start_command(update, context)
    
    elif any(word in user_text for word in ["reporte", "grafica"]):
        await show_reports_menu(update, context)
    else:
        await update.message.reply_text("No estoy seguro de cómo responder a eso, pero puedes intentar enviándome un CSV para procesar. ¡Usa /start para ver mis opciones!")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja las interacciones de los botones en los menús de Telegram."""
    query = update.callback_query
    await query.answer()
    
    if query.data == 'menu_reports':
        await show_reports_menu(update, context)
    
    elif query.data == 'main_menu':
        await start_command(update, context)

    elif query.data == 'csv_info':
        await query.message.reply_text(
            "**GUÍA DE INGESTA:**\n\n"
            "Envía un archivo `.csv` que contenga las 15 columnas requeridas:\n"
            "`Name`, `Age`, `Gender`, `Blood Type`, `Medical Condition`, `Date of Admission`, "
            "`Doctor`, `Hospital`, `Insurance Provider`, `Billing Amount`, `Room Number`, "
            "`Admission Type`, `Discharge Date`, `Medication`, `Test Results`.\n\n"
            "El sistema se encargará de la limpieza, normalización y carga automática."
        )

    elif query.data.startswith('q'):
        await generate_specific_report(query, query.data)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Gestiona la recepción de archivos CSV, valida su esquema y ejecuta el pipeline ."""
    file = update.message.document
    if not file.file_name.endswith('.csv'):
        await update.message.reply_text("Formato no válido. Por favor envía un archivo **CSV**.")
        return

    status_msg = await update.message.reply_text("**Documento recibido.** Iniciando carga en zona temporal...")
    new_file = await context.bot.get_file(file.file_id)
    temp_path = "temp_load.csv"
    await new_file.download_to_drive(custom_path=temp_path)
    
    try:
        # --- VALIDACIÓN DE ESQUEMA ---
        required_columns = [
            'Name', 'Age', 'Gender', 'Blood Type', 'Medical Condition', 
            'Date of Admission', 'Doctor', 'Hospital', 'Insurance Provider', 
            'Billing Amount', 'Room Number', 'Admission Type', 'Discharge Date', 
            'Medication', 'Test Results'
        ]
        
        df_valid = pd.read_csv(temp_path, nrows=0)
        missing_cols = [col for col in required_columns if col not in df_valid.columns]
        
        if missing_cols:
            error_msg = f"**Error de Esquema:** Al archivo le faltan las columnas:\n`{', '.join(missing_cols)}`"
            await status_msg.edit_text(error_msg, parse_mode='Markdown')
            if os.path.exists(temp_path): os.remove(temp_path)
            return

        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(script_dir, "..", "data")
        
        # --- CREACIÓN AUTOMÁTICA DE DIRECTORIO ---
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            print(f"Directorio creado: {data_dir}")

        target_path = os.path.join(data_dir, "healthcare_dataset.csv")
        os.replace(temp_path, target_path)
        
        await status_msg.edit_text("⚙️ **EJECUTANDO PIPELINE ...**")
        
        # Ejecutamos el pipeline
        await asyncio.to_thread(pipeline.run_end_to_end_pipeline, DATABASE_URL)
        
        await status_msg.edit_text("✅ **DATA PIPELINE COMPLETADO.** Generando Insights...")
        
        # --- REPORTE AUTOMÁTICO ---
        await send_automated_executive_report(update)
        
    except Exception as e:
        await status_msg.edit_text(f"**FALLO EN PIPELINE:**\n{str(e)}")
    finally: # Ensure temp file is removed even if pipeline fails
        if os.path.exists(temp_path): os.remove(temp_path)

if __name__ == '__main__':
    print("Chatbot Online y Escuchando...")
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    
    # Handlers
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    app.run_polling()
