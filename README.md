# Spotify Records Scraper

A scraper that collects data about Spotify streaming records from Wikipedia and generates visualizations.

## Key Features
- Extracts daily data from the "Most-streamed songs" table on Wikipedia
- Stores data in SQLite
- Generates song trend charts and top 10 rankings
- Scheduled daily execution

## Database Structure
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

## How to Run
### 1. Clone the repository:
```
git clone https://github.com/aristi95/spotify-scraper.git
cd spotify-scraper
```

### 2. Install dependencies:
```
pip install -r requirements.txt
```

### 3. Run the scraper:
```
python app.py
```

## Configuration
- Automatically runs daily at 3:00 PM (15:00).
You can also configure Windows Task Scheduler to run the code at specific intervals.

- Generated charts:
  
song_evolution_YYYY-MM-DD.png

top10_songs_YYYY-MM-DD.png

- Logs: spotify_scraper.log

## Customization
To change the song to monitor:

- Edit app.py and look for the 'song' variable in the 'generate_daily_charts' function

- Replace it with the name of the song you want to track


