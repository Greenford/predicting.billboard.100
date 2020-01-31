from lyricsgenius import Genius
from pymongo import MongoClient
from io import StringIO
import sys, sqlite3, time
import pandas as pd
import traceback as tb
from requests.exceptions import ReadTimeout

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout

class Scraper:

    def __init__(self, genius_auth_path='data/genius.auth', minsleep = 0.5):
        with open('data/genius.auth', 'r') as file:
            client_access_token = file.read().strip()
        self.minsleep = minsleep
        self.api = Genius(client_access_token, 
                          remove_section_headers=True)
        self.lyrics = MongoClient('localhost', 27017).tracks.lyrics
        self.errlog = MongoClient('localhost', 27017).tracks.errlog
        #df = pd.read_csv('data/MILLION_SONGS.csv', index_col=0)[['title', 'artist_name']]
        
        conn = sqlite3.connect('/mnt/snap/AdditionalFiles/track_metadata.db')
        q = '''SELECT track_id, title, artist_name, year FROM songs 
               WHERE year >= 1958 ORDER BY year DESC;'''
        df = pd.read_sql_query(q, conn)
        #df['title'] = df['title'].apply(lambda s: s[2:-1])
        #df['artist_name'] = df['artist_name'].apply(lambda s: s[2:-1])
        self.df = df

    def scrape_df_segment_to_db(self, start_idx, end_idx, verbose=False):
        df = self.df.copy()
        for i in range(start_idx, end_idx):
            row = df.iloc[i]
            #try:
            self.scrape_song_to_db(row['artist_name'],
                                   row['title'],
                                   row['track_id'])
            #except TypeError as e:
            #    self.record_error_verbose(self, row['track_id'], 
            #            ''.join(tb.format_list(tb.extract_tb()))+f'\n {TypeError}: {e.args}')
            if verbose:
                print(i)

    def scrape_song_to_db(self, artist, title, track_id):
        try:
            #record stout from lyricsgenius call because it catches errors and prints
            with Capturing() as output:
                songdata = self.api.search_song(title, artist)

        #for the few errors that have been raised
        except ReadTimeout as e:
            self.api.sleep_time += 3
            print(f'sleep time increased to {self.api.sleep_time}')
            self.record_error(track_id, 'ReadTimeout')
            self.scrape_song_to_db(artist, title, track_id)
            return

        #take sleep time slowly back to minimum
        if self.api.sleep_time > self.minsleep:
            self.api.sleep_time -= 0.25
            print(f'sleep time decreased to {self.api.sleep_time}')
        
        #search successful
        if songdata != None:
            self.record_lyrics_result(track_id, songdata)

        #handle (record & retry) Timeout error
        elif output[1].startswith('Timeout'):
            self.api.sleep_time += 3 
            self.record_error(track_id, 'Timeout')
            self.scrape_song_to_db(artist, title, track_id)
            return

        #record error: not in genius db
        elif output[1].startswith('No results'):
            self.record_error(track_id, 'no_results')
        
        #record error: song without lyrics
        elif output[1] == 'Specified song does not contain lyrics. Rejecting.':
            self.record_error(track_id, 'lacks_lyrics')
        
        #record error: URL issue
        elif output[1] == 'Specified song does not have a valid URL with lyrics. Rejecting.': 
            self.record_error(track_id, 'invalid_url')

    def record_lyrics_result(self, track_id, songdata):
        self.lyrics.insert_one({
            '_id': track_id,
            'response_artist': songdata.artist,
            'response_title': songdata.title,
            'lyrics': songdata.lyrics})

    def record_error(self, track_id, errtype):
        self.errlog.insert_one({
            'track': track_id,
            'type': errtype})

    def record_error_verbose(self, track_id, errmsg):
        self.errlog.insert_one({
            'track': track_id,
            'type': 'verbose',
            'message': errmsg})

if __name__ == '__main__':
    s = Scraper()

    start = int(sys.argv[1])
    end = int(sys.argv[2])

    s.scrape_df_segment_to_db(start, end, verbose=True)
    

