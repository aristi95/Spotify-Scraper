# Spotify Records Scraper

Un scraper que recopila datos de los récords de streaming de Spotify desde Wikipedia y genera visualizaciones.

## Características principales
- Extrae datos diarios de la tabla "Most-streamed songs" de Wikipedia
- Almacena los datos en SQLite
- Genera gráficos de evolución de canciones y top 10
- Ejecución programada diaria

## Estructura de la base de datos
```sql
CREATE TABLE spotify_records (
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
```

## Cómo ejecutar
### 1. Clona el repositorio:
```
git clone https://github.com/aristi95/spotify-scraper.git
cd spotify-scraper
```

### 2. Instala las dependencias:
```
pip install -r requirements.txt
```

### 3. Ejecuta el scraper:
```
python app.py
```

## Configuración
- Se ejecuta automáticamente cada día a las 15:00 (3 PM).
También se puede configurar Task Scheduler de Windows para que ejecute el código cada cierto tiempo. 

- Gráficos generados:

song_evolution_YYYY-MM-DD.png

top10_songs_YYYY-MM-DD.png

- Logs: spotify_scraper.log

## Personalización
Para cambiar la canción a monitorear:

- Edita app.py y busca la variable 'song' en la función 'generate_daily_charts'

- Reemplaza con el nombre de la canción que desees


