import os
import requests
from bs4 import BeautifulSoup
import logging
import re
from typing import Optional, Union
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta
import schedule
import time as tm
import logging.handlers
from pathlib import Path

# Configuración de logging
log_handler = logging.handlers.RotatingFileHandler(
    'spotify_scraper.log',
    maxBytes=5*1024*1024,  # 5 MB
    backupCount=3
)
logging.basicConfig(
    handlers=[log_handler],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Configuración de la base de datos
DATABASE_URL = "sqlite:///spotify_records.db"

def safe_int(value: str) -> Optional[int]:
    """Convierte a int de forma segura"""
    if not value:
        return None
    try:
        return int(float(value.strip().replace(',', '')))
    except (ValueError, TypeError):
        return None

def safe_float(value: str) -> Optional[float]:
    """Convierte a float de forma segura"""
    if not value:
        return None
    try:
        return float(value.strip().replace(',', ''))
    except (ValueError, TypeError):
        return None

def parse_year(year_text: str) -> Optional[int]:
    """Extrae el año de diferentes formatos"""
    if not year_text:
        return None
    
    # Limpia referencias [xx]
    year_text = re.sub(r'\[.*\]', '', year_text).strip()
    
    try:
        # Busca patrones de año (4 dígitos)
        year_match = re.search(r'\b(19|20)\d{2}\b', year_text)
        if year_match:
            return int(year_match.group())
        
        # Intenta convertir directamente si es solo número
        if year_text.isdigit():
            return int(year_text)
        
        # Para formatos de fecha como "29 November 2019"
        date_parts = year_text.split()
        if len(date_parts) >= 3:
            return int(date_parts[-1])
            
    except (ValueError, AttributeError):
        pass
    
    return None

def parse_date(date_text: str) -> Optional[str]:
    """Intenta parsear diferentes formatos de fecha"""
    if not date_text:
        return None
    
    try:
        # Para fechas como "29 November 2019"
        date_obj = datetime.strptime(date_text, '%d %B %Y')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        return None

def create_database():
    """Crea la base de datos y la tabla si no existen"""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS spotify_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scraping_date DATE NOT NULL,
                rank INTEGER NOT NULL,
                song TEXT NOT NULL,
                artist TEXT NOT NULL,
                streams REAL,
                release_year INTEGER,
                daily_average REAL,
                record_date TEXT,
                days_on_record INTEGER,
                UNIQUE(scraping_date, rank, song, artist)
            )
            """))
            conn.commit()
        logging.info("Base de datos verificada/creada correctamente")
    except Exception as e:
        logging.error(f"Error al crear la base de datos: {str(e)}")
        raise

def scrape_spotify_records():
    # Limpiar datos del día actual para evitar duplicados
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("""
        DELETE FROM spotify_records 
        WHERE scraping_date = :today
        """), {'today': datetime.now().date().strftime('%Y-%m-%d')})
        conn.commit()
        
    """Función principal para hacer scraping de los records de Spotify"""
    logging.info(f"Iniciando scraping - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    url = "https://en.wikipedia.org/wiki/List_of_Spotify_streaming_records"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        # 1. Obtener el contenido de la página
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 2. Encontrar la tabla "Most-streamed songs"
        target_table = None
        for table in soup.find_all('table', {'class': 'wikitable'}):
            prev_header = table.find_previous('h2')
            if prev_header and "Most-streamed songs" in prev_header.text:
                target_table = table
                break
                
        if not target_table:
            raise ValueError("No se encontró la tabla 'Most-streamed songs'")
        
        # 3. Extraer datos de la tabla
        rankings = []
        rows = target_table.find_all('tr')[1:]  # Saltar el encabezado
        
        for row in rows:
            cols = row.find_all(['th', 'td'])
            if len(cols) < 6:  # Asegurarse de que hay suficientes columnas
                continue
                
            try:
                # Extraer y limpiar datos
                rank = safe_int(cols[0].text)
                song = cols[1].text.strip().replace('"', '')
                artist = cols[2].text.strip()
                
                # Streams (maneja billones/millones si es necesario)
                streams_text = cols[3].text.split('[')[0].strip().lower()
                if 'billion' in streams_text:
                    streams = safe_float(streams_text.replace(' billion', '')) * 1e9
                elif 'million' in streams_text:
                    streams = safe_float(streams_text.replace(' million', '')) * 1e6
                else:
                    streams = safe_float(streams_text.replace(',', ''))
                
                # Fecha de lanzamiento
                release_year = parse_year(cols[4].text)
                
                # Promedio diario
                daily_avg_text = cols[5].text.split('[')[0].strip().lower()
                daily_avg = safe_float(daily_avg_text.replace(',', ''))
                
                # Fecha del récord
                record_date = None
                date_span = cols[3].find('span', {'class': 'date-style'})
                if date_span:
                    record_date = parse_date(date_span.text.strip())
                
                # Días en el récord
                days_text = cols[6].text.split('[')[0].strip() if len(cols) > 6 else None
                days_on_record = safe_int(days_text)
                
                rankings.append({
                    'scraping_date': datetime.now().date().strftime('%Y-%m-%d'),
                    'rank': rank,
                    'song': song,
                    'artist': artist,
                    'streams': streams,
                    'release_year': release_year,
                    'daily_average': daily_avg,
                    'record_date': record_date,
                    'days_on_record': days_on_record
                })
                
            except Exception as e:
                logging.warning(f"Error procesando fila: {str(e)}\nContenido: {cols}")
                continue
        
        # 4. Guardar en la base de datos
        if rankings:
            engine = create_engine(DATABASE_URL)
            df = pd.DataFrame(rankings)
            df.to_sql('spotify_records', engine, if_exists='append', index=False)
            logging.info(f"Datos guardados correctamente - {len(rankings)} registros")
            
            # Generar visualizaciones
            generate_daily_charts()
        else:
            logging.warning("No se encontraron datos para guardar")
            
    except Exception as e:
        logging.error(f"Error durante el scraping: {str(e)}")
        raise

def generate_daily_charts():
    """Genera ambos gráficos actualizados con datos del día"""
    try:
        conn = sqlite3.connect('spotify_records.db')
        current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        
        # --- Gráfico 1: Evolución de una canción del ranking ---

        # Crear carpeta para guardar los gráficos
        # Obtener el directorio actual del script
        directorio_actual = Path(__file__).parent
        # Nombre de la carpeta a crear
        nombre_carpeta = "charts"
        # Ruta completa de la nueva carpeta
        ruta_carpeta = directorio_actual / nombre_carpeta
        # Crear la carpeta si no existe
        ruta_carpeta.mkdir(exist_ok=True)

        #Canción
        song = 'Die With A Smile'
        
        # Consulta para obtener todos los datos de la canción
        query = f"""
        SELECT scraping_date, rank, streams, daily_average 
        FROM spotify_records 
        WHERE song LIKE '%{song}%' 
        ORDER BY scraping_date
        """
        df = pd.read_sql(query, conn)
        
        if not df.empty:
            # 1. Guardar datos en CSV (append mode)
            csv_filename = 'charts/song_evolution_data.csv'
            df.to_csv(csv_filename, mode='a', header=not os.path.exists(csv_filename), index=False)
            
            # 2. Procesar datos para el gráfico
            df['scraping_date'] = pd.to_datetime(df['scraping_date'])
            
            # Crear gráfico
            fig, ax1 = plt.subplots(figsize=(12, 6))
            
            # Gráfico de ranking
            ax1.plot(df['scraping_date'], df['rank'], 'ro-', label='Ranking')
            ax1.invert_yaxis()
            ax1.set_xlabel('Fecha')
            ax1.set_ylabel('Ranking', color='red')
            ax1.tick_params(axis='y', labelcolor='red')
            
            # Gráfico de streams (eje secundario)
            ax2 = ax1.twinx()
            ax2.plot(df['scraping_date'], df['streams']/1e9, 'b--', label='Streams (B)')
            ax2.plot(df['scraping_date'], df['daily_average']/1e6, 'g:', label='Avg. Diario (M)')
            ax2.set_ylabel('Streams', color='blue')
            ax2.tick_params(axis='y', labelcolor='blue')
            
            # Título y leyenda unificada
            plt.title(f'Evolución de "{song}" en Spotify\nÚltima actualización: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
            
            # Guardar siempre con el mismo nombre (sobrescribe)
            plt.savefig('charts/song_evolution_current.png', dpi=150, bbox_inches='tight')
            plt.close()
            
            print(f"Gráfico 1 actualizado: song_evolution_current.png")
            print(f"Datos guardados en: {csv_filename}")
        else:
            print(f"No hay datos para '{song}'")

        # --- Gráfico 2: Top 10 canciones del día ---
        df_top10 = pd.read_sql("""
            SELECT song, artist, streams 
            FROM spotify_records 
            WHERE scraping_date = (SELECT MAX(scraping_date) FROM spotify_records)
            ORDER BY rank 
            LIMIT 10
        """, conn)

        if not df_top10.empty:
            plt.figure(figsize=(12, 7))
            bars = plt.barh(
                df_top10['song'] + '\n' + df_top10['artist'],                
                df_top10['streams'],
                color='#1DB954',
                height=0.7
            )
            
            # Añadir etiquetas de valor
            for bar in bars:
                width = bar.get_width()
                plt.text(width, bar.get_y() + bar.get_height()/2, 
                        f'{width:.2f}B', 
                        ha='left', va='center',
                        fontsize=10)
            
            plt.title(f'Top 10 Canciones en Spotify\nActualizado: {datetime.now().strftime("%Y-%m-%d %H:%M")}', fontsize=14, pad=20)
            plt.xlabel('Streams (miles de millones)', fontsize=12)
            plt.xticks(fontsize=10)
            plt.yticks(fontsize=10)
            plt.tight_layout()
            plt.savefig(f'charts/top10_songs_{current_datetime}.png', dpi=150, bbox_inches='tight')
            plt.close()

        print(f"Gráficos actualizados generados en {current_datetime}")

    except Exception as e:
        print(f"Error generando gráficos: {str(e)}")
        logging.error(f"Error generando gráficos: {str(e)}")
    finally:
        conn.close()
        
def run_daily_task():
    """Ejecuta las tareas diarias programadas con manejo de errores"""
    try:
        logging.info("="*50)
        logging.info("Iniciando ejecución diaria")
        
        create_database()
        scrape_spotify_records()
        
        logging.info("Ejecución completada exitosamente")
        logging.info("="*50 + "\n")
    except Exception as e:
        logging.error(f"Error en la ejecución diaria: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    # Configurar el scheduler para ejecución diaria
    schedule.every().day.at("15:00").do(run_daily_task)  # Ejecutar a las 3 pm
    
    # Ejecutar inmediatamente la primera vez
    run_daily_task()
    
    print("Scraper iniciado. Presiona Ctrl+C para salir.")
    print(f"Próxima ejecución programada: {schedule.next_run()}")
    
    try:
        while True:
            schedule.run_pending()
            tm.sleep(60)  # Revisar cada minuto si hay tareas pendientes
    except KeyboardInterrupt:
        print("Scraper detenido")
