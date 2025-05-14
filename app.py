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

# Configuración de logging
logging.basicConfig(
    filename='spotify_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
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
        today = datetime.now().strftime('%Y-%m-%d')
        
        # --- Gráfico 1: Evolución de "Die With A Smile" ---
        # Consulta para obtener todos los datos de la canción
        query = """
        SELECT scraping_date, rank, streams, daily_average 
        FROM spotify_records 
        WHERE song LIKE '%Die With A Smile%' 
        ORDER BY scraping_date
        """
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print("No se encontraron datos para 'Die With A Smile'")
            return
            
        # Convertir fechas
        df['scraping_date'] = pd.to_datetime(df['scraping_date'])
        
        # Crear gráfico con dos ejes y
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        # Gráfico de ranking (eje y invertido)
        color = 'tab:red'
        ax1.set_xlabel('Fecha de seguimiento')
        ax1.set_ylabel('Ranking', color=color)
        ax1.plot(df['scraping_date'], df['rank'], color=color, marker='o', label='Ranking')
        ax1.invert_yaxis()  # Para que #1 sea arriba
        ax1.tick_params(axis='y', labelcolor=color)
        ax1.grid(True, alpha=0.3)
        
        # Gráfico de streams (eje y secundario)
        ax2 = ax1.twinx()  
        color = 'tab:blue'
        ax2.set_ylabel('Streams (miles de millones)', color=color)
        ax2.plot(df['scraping_date'], df['streams']/1e9, color=color, 
                linestyle='--', marker='s', label='Streams Totales')
        ax2.tick_params(axis='y', labelcolor=color)
        
        # Gráfico de promedio diario
        color = 'tab:green'
        ax2.plot(df['scraping_date'], df['daily_average'], color=color, 
                linestyle=':', marker='^', label='Promedio Diario')
        
        # Añadir título y leyenda
        plt.title('Evolución diaria de "Die With A Smile" en Spotify')
        fig.tight_layout()
        
        # Combinar leyendas
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        
        # Guardar con fecha actual
        today = datetime.now().strftime('%Y-%m-%d')
        filename = f'diewithasmile_evolution_{today}.png'
        plt.savefig(filename, dpi=120)
        plt.close()

        # --- Gráfico 2: Top 10 canciones del día ---
        df_top10 = pd.read_sql("""
            SELECT song, artist, streams 
            FROM spotify_records 
            WHERE scraping_date = (SELECT MAX(scraping_date) FROM spotify_records)
            ORDER BY rank 
            LIMIT 10
        """, conn)

        if not df_top10.empty:
            plt.figure(figsize=(10, 6))
            bars = plt.barh(
                df_top10['song'] + ' - ' + df_top10['artist'],
                df_top10['streams'] / 1e9,
                color='#1DB954'
            )
            
            # Añadir etiquetas de valor
            for bar in bars:
                width = bar.get_width()
                plt.text(width, bar.get_y() + bar.get_height()/2, 
                        f'{width:.1f}B', 
                        ha='left', va='center')
            
            plt.title(f'Top 10 Canciones en Spotify - {today}')
            plt.xlabel('Streams (miles de millones)')
            plt.tight_layout()
            plt.savefig(f'top10_songs_{today}.png', dpi=120)
            plt.close()

        print(f"Gráficos actualizados para {today}")

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        conn.close()
        
def run_daily_task():
    """Ejecuta las tareas diarias programadas"""
    create_database()
    scrape_spotify_records()

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
