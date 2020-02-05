import pandas as pd
import numpy as np
import sqlite3
from pymongo import MongoClient
import os

def run_match():
    col = MongoClient('localhost', 27017).tracks.audio_features
    result = col.find({'_id':{'$exists':'true'}, 'audio_features':{'$exists':'true'}}, 
        {'_id':'true'})

    MSDIDs = {r['_id'] for r in result}
    print('Relevant MSDIDs identified')
    
    meta = get_metadata_for_MSDIDs(MSDIDs)
    print('Metadata DB read')

    bb = get_billboard_data()
    print('billboard data read')

    for year in bb['year'].unique():
        print(year)
        bb_year = bb[bb['year'] == year]
        for i in range(bb_year.shape[0]):
            bb_song = bb_year.iloc[i]
            exact = meta[(meta['year'] == year)\
                & (meta['title']==bb_song['track'])\
                & (meta['artist_name']==bb_song['artist'])]
            if exact.shape[0] > 0:
                bb.iloc[int(bb_song.index[0])]['msdid'] = exact.iloc[0]['track_id']
    bb = bb[~bb['msdid'].isna()]
    bb.drop(columns='year', inplace=True)
    bb.to_csv('data/Billboard_MSD_Matches.csv')
    print('Complete')

def get_billboard_data():
    bb = pd.read_csv("data/HotSongsBillBoard.csv")
    bb['year'] = list(map(lambda dt: dt.year, bb['publish_date'].astype('datetime64')))
    bb = bb[bb['year'] <= 2010]
    bb.insert(4, 'msdid', np.nan)
    return bb

def get_metadata_for_MSDIDs(MSDID_set):
    conn = sqlite3.connect('/mnt/snap/AdditionalFiles/track_metadata.db')
    q = '''SELECT track_id,
                  title,
                  artist_name,
                  year
           FROM songs
           WHERE year >= 1958;'''
    df = pd.read_sql_query(q, conn)
    mask = [(MSDID in MSDID_set) for MSDID in df['track_id'].values]
    df = df[mask]
    return df


if __name__ == '__main__':
    run_match()
