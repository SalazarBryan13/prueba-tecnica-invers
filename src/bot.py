import os
import asyncio
import pandas as pd
import plotly.express as px
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Configuración
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# Importar pipeline
import importlib
pipeline = importlib.import_module("04_run_pipeline")

engine = create_engine(DATABASE_URL)

# --- MOTOR DE REPORTES ---

async def send_automated_executive_report(update: Update):
    """Genera un reporte completo KPIs + Visualización tras la ingesta."""
    try:
        # 1. Obtener KPIs
        query_kpis = """
        SELECT 
            COUNT(*) as total_records,
            SUM(billing_amount) as total_billing,
            (SELECT COUNT(*) FROM fact_admission WHERE test_results = 'Abnormal') * 100.0 / COUNT(*) as abnormal_rate
        FROM fact_admission
        """
        kpis = pd.read_sql(query_kpis, engine).iloc[0]
        
        kpi_text = (
            "📊 **REPORTE EJECUTIVO AUTOMÁTICO**\n"
            "--- Ingesta Exitosa ---\n\n"
            f"✅ **Registros Totales:** {int(kpis['total_records']):,}\n"
            f"💰 **Facturacion Total:** ${kpis['total_billing']/1e6:.2f}M\n"
            f"⚠️ **Tasa Alertas (Abnormal):** {kpis['abnormal_rate']:.1f}%\n"
            "--------------------------"
        )
        
        # 2. Generar Gráfico de Resumen (Top 5 Hospitales)
        query_chart = """
        SELECT h.hospital_name, SUM(f.billing_amount) as total 
        FROM fact_admission f 
        JOIN dim_hospital h ON f.hospital_id = h.hospital_id 
        GROUP BY h.hospital_name ORDER BY total DESC LIMIT 5
        """
        df = pd.read_sql(query_chart, engine)
        fig = px.bar(df, x='hospital_name', y='total', title='Estado Actual: Top 5 Hospitales', color='total')
        
        img_path = "auto_report_summary.png"
        fig.write_image(img_path)
        
        # 3. Enviar todo
        await update.message.reply_photo(
            photo=open(img_path, 'rb'), 
            caption=kpi_text
        )
        os.remove(img_path)
        
    except Exception as e:
        await update.message.reply_text(f"⚠️ Reporte automático falló: {str(e)}")

async def generate_specific_report(update_source, report_type):
    """Genera reportes específicos del menú bajo demanda."""
    message = update_source.message if hasattr(update_source, 'message') else update_source
    
    try:
        if report_type == 'q1_seasonality':
            query = """
            SELECT d.year, d.month, COUNT(*) as admisiones
            FROM fact_admission f
            JOIN dim_date d ON f.admission_date_id = d.date_id
            GROUP BY d.year, d.month ORDER BY d.year, d.month
            """
            df = pd.read_sql(query, engine)
            df['Periodo'] = df['year'].astype(str) + "-" + df['month'].astype(str).str.zfill(2)
            fig = px.line(df, x='Periodo', y='admisiones', markers=True, title="Q1: Tendencia de Admisiones")
            caption = "📈 **Análisis de Estacionalidad**"
            
        elif report_type == 'q2_meds':
            query = """
            SELECT m.medication_name, SUM(f.billing_amount) as total
            FROM fact_admission f
            JOIN dim_medication m ON f.medication_id = m.medication_id
            GROUP BY m.medication_name ORDER BY total DESC LIMIT 10
            """
            df = pd.read_sql(query, engine)
            fig = px.bar(df, x='total', y='medication_name', orientation='h', title="Q2: Top 10 Medicamentos", color='total')
            caption = "💊 **Impacto de Medicamentos**"

        elif report_type == 'q5_insurance':
            query = """
            SELECT i.provider_name, SUM(f.billing_amount) as total
            FROM fact_admission f
            JOIN dim_insurance i ON f.insurance_id = i.insurance_id
            GROUP BY i.provider_name ORDER BY total DESC
            """
            df = pd.read_sql(query, engine)
            fig = px.pie(df, values='total', names='provider_name', title="Q5: Facturación por Aseguradora")
            caption = "🛡️ **Distribución por Seguros**"

        img_path = f"manual_{report_type}.png"
        fig.write_image(img_path)
        await message.reply_photo(photo=open(img_path, 'rb'), caption=caption)
        os.remove(img_path)
        
    except Exception as e:
        await message.reply_text(f"⚠️ Error: {str(e)}")

# --- HANDLERS ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📊 Menú de Reportes", callback_data='menu_reports')],
        [InlineKeyboardButton("📁 Instrucciones CSV", callback_data='csv_info')]
    ]
    await update.message.reply_text(
        "💪 **BOT MLOPS AUTOMATIZADO**\n\n"
        "Sistema configurado para **Reporte Ejecutivo Automático** tras cada carga exitosa.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == 'menu_reports':
        keyboard = [
            [InlineKeyboardButton("📈 Estacionalidad (Q1)", callback_data='q1_seasonality')],
            [InlineKeyboardButton("💊 Top Medicamentos (Q2)", callback_data='q2_meds')],
            [InlineKeyboardButton("🛡️ Seguros (Q5)", callback_data='q5_insurance')],
            [InlineKeyboardButton("🔙 Inicio", callback_data='main_menu')]
        ]
        await query.edit_message_text("🔍 **ELIGE UN REPORTE:**", reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif query.data == 'main_menu':
        await start_command(query, context)

    elif query.data == 'csv_info':
        await query.message.reply_text("📁 **ENVÍA UN CSV:** Debe tener columnas `Name`, `Hospital`, `Billing Amount`.")

    elif query.data.startswith('q'):
        await generate_specific_report(query, query.data)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = update.message.document
    if not file.file_name.endswith('.csv'):
        await update.message.reply_text("❌ Solo archivos CSV.")
        return

    status_msg = await update.message.reply_text("📝 Iniciando procesamiento...")
    new_file = await context.bot.get_file(file.file_id)
    temp_path = "temp_load.csv"
    await new_file.download_to_drive(custom_path=temp_path)
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        target_path = os.path.join(script_dir, "..", "data", "healthcare_dataset.csv")
        os.replace(temp_path, target_path)
        
        await status_msg.edit_text("⚙️ **EJECUTANDO PIPELINE ELT...**")
        await asyncio.to_thread(pipeline.run_end_to_end_pipeline, DATABASE_URL)
        
        await status_msg.edit_text("✅ **PIPELINE FINALIZADO CON ÉXITO.**")
        
        # --- REPORTE AUTOMÁTICO ---
        await send_automated_executive_report(update)
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")
        if os.path.exists(temp_path): os.remove(temp_path)

if __name__ == '__main__':
    print("🤖 Bot Proactivo Escuchando...")
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.run_polling()