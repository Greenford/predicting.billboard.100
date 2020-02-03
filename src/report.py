import numpy as np
from pymongo import MongoClient

class genius_report:

    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.lyrics = client.tracks.lyrics
        self.errlog = client.tracks.errlog

    def num_scraped(self):
        l = self.lyrics.estimated_document_count()
        e = self.errlog.estimated_document_count()
        print(f'{l} song lyrics gathered')
        print(f'{e} errors encountered')
        print(f'{l+e} total tracks tried with {np.around(100*l/(l+e),2)}% success')
    
if __name__ == '__main__':
    r = genius_report()
    r.num_scraped()
