import numpy as np
from pymongo import MongoClient

class genius_report:

    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.lyrics = client.tracks.lyrics
        self.errlog = client.tracks.errlog

        #self.lyrics_count = lyrics.estimated_document_count()
        #len(np.array([int(i['_id']) for i in lyrics.find(\
        #    {'_id': {'$exists':'true'}}, {'_id':'true'})]))
        #self.err_count = errlog
        #len(np.array([int(i['_id']) for i in errlog.find(\
        #    {'_id': {'$exists':'true'}}, {'_id':'true'})]))

    def num_scraped(self):
        l = self.lyrics.estimated_document_count()
        e = self.errlog.estimated_document_count()
        print(f'{l} song lyrics gathered')
        print(f'{e} errors encountered')
        print(f'{l+e} total tracks tried with {np.around(100*l/(l+e),2)}% success')
    
if __name__ == '__main__':
    r = genius_report()
    r.num_scraped()
