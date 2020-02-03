import numpy as np
from pymongo import MongoClient

class genius_report:

    def __init__(self):
        self.db = MongoClient('localhost', 27017).tracks

    def num_lyrics_scraped(self):
        l = self.db.lyrics.estimated_document_count()
        e = self.db.errlog.estimated_document_count()
        print('Genius:')
        print(f'{l} song lyrics successfully gathered')
        print(f'{l+e} total tracks tried with {np.around(100*l/(l+e),2)}% success')
    
    def num_spotify_tracks_scraped(self):
        n = self.db.audio_features.estimated_document_count()
        e = self.db.audio_errlog.estimated_document_count()
        print('Spotify:')
        print(f'{n} spotify track audio features successfully gathered')
        print(f'{n+e} total tracks tried with {np.around(100*n/(l+e),2)}% success')

if __name__ == '__main__':
    r = genius_report()
    r.num_lyrics_scraped()
    print()
    r.num_spotify_tracks_scraped()

