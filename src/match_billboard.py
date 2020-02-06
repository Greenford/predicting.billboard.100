import pandas as pd
import numpy as np
import sqlite3
from pymongo import MongoClient
import os

def run_match():
    col = MongoClient('localhost', 27017).tracks.audio_features
    result = col.find({'_id':{'$exists':'true'}, 'audio_features':{'$exists':'true'}}, 
        {'_id':'true'})

    MSDID_set = {r['_id'] for r in result}
    print('Relevant MSDIDs identified')
    
    meta = get_metadata_for_MSDIDs(MSDID_set)
    print('Metadata DB read')

    bb = get_billboard_data()
    print('billboard data read')
    
    bb = exact_match(bb, meta)

    bb = bb[bb['msdid']!= '']
    bb.drop(columns='year', inplace=True)
    
    print(f'{bb.shape[0]} matches made with exact')
    bb.to_csv('data/Billboard_MSD_Matches.csv')

def exact_match(bb, meta):
    for i in bb.index:
        row = bb.loc[i]
        metarow = meta[(meta['year']==row['year']) & (meta['artist_name']==row['artist'])\
                & (meta['title']==row['track'])]
        if metarow.shape[0] > 0:
            msdid = metarow['track_id']
            bb.at[i,'msdid'] = metarow.iloc[0]['track_id']
    return bb


def get_billboard_data():
    bb = pd.read_csv("data/HotSongsBillBoard.csv")
    bb['year'] = list(map(lambda dt: dt.year, bb['publish_date'].astype('datetime64')))
    bb = bb[bb['year'] <= 2010]
    bb.insert(4, 'msdid', '')
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

def run_match_all():
    meta = get_metadata()
    print('Metadata DB read')

    bb = get_billboard_data()
    print('billboard data read')

    bb = exact_match(bb, meta)

    bb = bb[bb['msdid']!= '']
    bb.drop(columns='year', inplace=True)
    print(f'{bb.shape[0]} matches made with exact')
    bb.to_csv('data/All_Billboard_MSD_Matches.csv')

def get_metadata():
    conn = sqlite3.connect('/mnt/snap/AdditionalFiles/track_metadata.db')
    q = '''SELECT track_id,
                  title,
                  artist_name,
                  year
           FROM songs;'''
    df = pd.read_sql_query(q, conn)
    return df


if __name__ == '__main__':
    run_match_all()
