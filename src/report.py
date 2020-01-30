import numpy as np
from pymongo import MongoClient

class genius_report:

    def __init__(self):
        client = MongoClient('localhost', 27017)
        lyrics = client.tracks.lyrics
        errlog = client.tracks.errlog

        self.lyrics_count = len(np.array([int(i['_id']) for i in lyrics.find(\
            {'_id': {'$exists':'true'}}, {'_id':'true'})]))
        self.err_count = len(np.array([int(i['_id']) for i in errlog.find(\
            {'_id': {'$exists':'true'}}, {'_id':'true'})]))

    def num_scraped(self):
        total = self.lyrics_count+self.err_count
        print(f'{self.lyrics_count} song lyrics gathered')
        print(f'{self.err_count} errors encountered')
        print(f'{total} total tracks tried with {100*self.lyrics_count/total}% success')
    
if __name__ == '__main__':
    r = genius_report()
    r.num_scraped()
