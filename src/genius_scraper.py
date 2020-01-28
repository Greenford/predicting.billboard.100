from lyricsgenius import Genius
from pymongo import MongoClient
import sys
import pandas as pd

class Scraper:

    def __init__(self, genius_auth_path='data/genius.auth'):
        with open('data/genius.auth', 'r') as file:
            client_access_token = file.read().strip()

        self.api = Genius(client_access_token, 
                          remove_section_headers=True)
        self.lyrics = MongoClient('localhost', 27017).genius.lyrics
        self.df = pd.read_csv('data/MILLION_SONGS.csv', index_col=0)[['title', 'artist_name']]
        df['title'] = df['title'].apply(lambda s: s[2:-1])
        df['artist_name'] = df['artist_name'].apply(lambda s: s[2:-1])
    
    def scrape_range_songs_to_db(self, irange):

        for i in irange:
            artist = self.df.iloc[i]['artist_name']
            title = self.df.iloc[i]['title']
            self.scrape_song_to_db(artist, title, i)

    def scrape_song_to_db(self, artist, title, index, verbose=True):
        songdata = self.api.search_song(title, artist)
        if songdata == None:
            print(f'Index {index} ERROR ABOVE')
        else:
            self.lyrics.insert_one({
                '_id': index,
                'searched_artist': artist,
                'response_artist': songdata.artist,
                'searched_title': title,
                'response_title': songdata.title,
                'lyrics': songdata.lyrics})
            if verbose:
                print(f'Index {index} successful')

if __name__ == '__main__':
    s = Scraper()

    start = int(sys.argv[1])
    end = int(sys.argv[2])
    irange = range(start, end)

    s.scrape_range_songs_to_db(irange)
    

