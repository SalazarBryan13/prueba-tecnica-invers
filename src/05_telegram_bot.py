import os
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

# Cargar variables desde el archivo .env en la raíz
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# Importamos TU orquestador existente
import importlib            
pipeline = importlib.import_module("04_run_pipeline")
    
# Coloca aquí tu token generado por BotFather
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Responde cuando el usuario escribe /start"""
    await update.message.reply_text(
        "¡Hola! Soy el Bot MLOps. Envíame el archivo 'healthcare_dataset.csv' "
        "y ejecutaré todo el proceso por ti."
    )
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Se dispara automáticamente cuando el bot recibe un documento (CSV)"""
    file = update.message.document
    
    # Validar que sea un archivo CSV válido
    if not file.file_name.endswith('.csv'):
        await update.message.reply_text("Error: Solo acepto archivos con formato .csv")
        return
    await update.message.reply_text("Descargando archivo. Iniciando proceso de ejecución.")
    
    # 1. Bajar el archivo usando Telegram API
    new_file = await context.bot.get_file(file.file_id)
    
    # 2. Guardarlo en la carpeta correcta, sobrescribiendo el viejo
    script_dir = os.path.dirname(os.path.abspath(__file__))
    target_path = os.path.join(script_dir, "..", "data", "healthcare_dataset.csv")
    
    try:
        await new_file.download_to_drive(custom_path=target_path)
    except PermissionError:
        await update.message.reply_text(
            "Error de Permiso: No puedo sobrescribir el archivo 'healthcare_dataset.csv'.\n\n"
            "Asegúrate de que el archivo NO esté abierto en Excel, Jupyter u otro programa y reintenta."
        )
        return
        
    await update.message.reply_text("Archivo recibido. Iniciando Fase 2 y 3 (Limpieza y Modelado)")
    
    try:
        # Ejecución del pipeline
        pipeline.run_end_to_end_pipeline(DATABASE_URL)
        
        await update.message.reply_text(
            "PIPELINE COMPLETADO EXITOSAMENTE!\n\n"
        )
    except Exception as e:
        await update.message.reply_text(f"Ocurrió un error en el pipeline:\n{str(e)}")
if __name__ == '__main__':
    print("Iniciando Listener de Telegram MLOps Bot...")
    
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    
    # Rutas o 'Endpoints' del bot
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    
    # Script corriendo
    app.run_polling()