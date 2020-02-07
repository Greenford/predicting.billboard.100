import pandas as pd
import numpy as np
import psycopg2 as pg2
from sklearn.model_selection import train_test_split

def read_clean_data1():
    conn = pg2.connect(dbname='billboard', host='localhost', user='postgres')
    cur = conn.cursor()

    q = '''
    SELECT msd_artist_familiarity, 
           msd_artist_hotttnesss, 
           msd_year, 
           msd_loudness, 
           msd_duration, 
           msd_key, 
           msd_key_confidence, 
           msd_mode, 
           msd_mode_confidence, 
           msd_song_hotttnesss,
           spot_valence,
           gen_dict_sentiment,
           on_billboard
    FROM tracks;'''
    cur.execute(q)
    data = np.array([row for row in cur])

    df = pd.DataFrame(data, columns=['msd_artist_familiarity', 
                                     'msd_artist_hotttnesss', 
                                     'msd_year', 
                                     'msd_loudness', 
                                     'msd_duration', 
                                     'msd_key', 
                                     'msd_key_confidence',
                                     'msd_mode',
                                     'msd_mode_confidence',
                                     'msd_song_hotttnesss',
                                     'spot_valence',
                                     'gen_dict_sentiment',
                                     'on_billboard'])
    floatcols = ['msd_artist_familiarity', 
                 'msd_artist_hotttnesss', 
                 'msd_loudness',
                 'msd_key_confidence', 
                 'msd_mode_confidence',
                 'msd_song_hotttnesss',
                 'spot_valence',
                 'gen_dict_sentiment']

    intcols = ['msd_year', 
               'msd_duration', 
               'msd_key', 
               'msd_mode',
               'on_billboard']
    df[floatcols] = df[floatcols].astype(float)
    df[intcols] = df[intcols].astype(int)

    # pre-split transformations
    df['msd_mode'] = df['msd_mode'].apply(lambda mode: -1 if (mode==0) else 1)
    df['mode'] = df['msd_mode']*df['msd_mode_confidence']
    df.drop(columns=['msd_mode', 'msd_mode_confidence'], inplace=True)
    df['emo_consistency']=np.abs(df['gen_dict_sentiment']-(df['spot_valence']*2-1))
    df.drop(columns=['spot_valence', 'gen_dict_sentiment'], inplace=True)
    
    train, test = train_test_split(df, test_size=0.2, random_state=77)

    # post-split transformations
    med = train['msd_song_hotttnesss'].median()
    train['msd_song_hotttnesss'].fillna(med, inplace=True) 
    test['msd_song_hotttnesss'].fillna(med, inplace=True)

    cur.close()
    conn.close()
    return train, test

def read_clean_data2():
    conn = pg2.connect(dbname='billboard', host='localhost', user='postgres')
    cur = conn.cursor()

    intcols = [ 'msd_year',
                'spot_key',
                'spot_mode',
                'spot_duration',
                'on_billboard']
    floatcols=[ 'spot_danceability', 
                'spot_energy', 
                'spot_loudness', 
                'spot_speechiness', 
                'spot_instrumentalness', 
                'spot_liveness', 
                'spot_valence', 
                'spot_tempo', 
                'msd_song_hotttnesss',
                'msd_artist_hotttnesss',
                'gen_dict_sentiment']
    allcolumns = intcols + floatcols
    q = f'SELECT {", ".join(allcolumns)} FROM tracks;'
    cur.execute(q)
    data = np.array([row for row in cur])

    df = pd.DataFrame(data, columns=allcolumns)
    df[floatcols] = df[floatcols].astype(float)
    df[intcols] = df[intcols].astype(int)

    # pre-split transformations
    df['sent_consistency']=np.abs(df['gen_dict_sentiment']-(df['spot_valence']*2-1))
    df['total_sent'] = df['gen_dict_sentiment']+(df['spot_valence']*2-1)
    train, test = train_test_split(df, test_size=0.2, random_state=77)

    # post-split transformations
    med = train['msd_song_hotttnesss'].median()
    train['msd_song_hotttnesss'].fillna(med, inplace=True)
    test['msd_song_hotttnesss'].fillna(med, inplace=True)

    cur.close()
    conn.close()
    return train, test

