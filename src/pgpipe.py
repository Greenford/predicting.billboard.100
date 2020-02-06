from pymongo import MongoClient
import sqlite3
import pandas as pd
import hdf5_getters as hdf5
import psycopg2 as pg2
from psycopg2.errors import StringDataRightTruncation
from time import time

def run_pipe():
    tracks = MongoClient('localhost', 27017).tracks
    print('MongoDB connection successful')

    af, MSDID_set = read_spotify_audio_features(tracks.audio_features)
    print('Spotify audio features ready for inserts')
    
    lyrics = read_genius_lyrics_by_MSDID(MSDID_set, tracks.lyrics)
    # TODO check/cut MSDID_set down
    print('Lyrics ready for inserts')

    metadata = read_metadata_by_MSDID(MSDID_set)
    # TODO check/cut MSDID_set down
    print('Metadata ready for inserts')

    _reset_pgdb()
    _create_tables()
    print('PostgreSQL DB readied')
    
    conn = pg2.connect(dbname='billboard', host='localhost', port=5432, user='postgres')
    cur = conn.cursor()

    for MSDID in MSDID_set:
        msd_row = get_MSD_fields(MSDID)
        insert_track(conn, cur, MSDID, af, lyrics, metadata, msd_row)

def read_spotify_audio_features(audio_feat_col):
    af = dict()
    MSDID_set = set()
    result = audio_feat_col.find({'_id':{'$exists':'true'}, 'audio_features':{'$exists':'true'}})
    af_keys_to_del = ['type', 'id', 'uri', 'track_href', 'analysis_url']
    meta_keys_to_keep = ['name', 'explicit']

    for track in result:
        current = dict()
        MSDID = track['_id']
        MSDID_set.add(MSDID)
        current = track['audio_features']

        for key in af_keys_to_del:
            del current[key]
        
        for key in meta_keys_to_keep:
            current[key] = track['metadata'][key]
        
        current['artist'] = track['metadata']['artists'][0]['name']
        current['release_date'] = track['metadata']['album']['release_date']
        af[MSDID] = current

        #if len(af)>=1000:
        #    break
    return af, MSDID_set

def read_genius_lyrics_by_MSDID(MSDID_set, lyrics_col):
    result = lyrics_col.find({'_id':{'$in': list(MSDID_set)}})
    lyrics = dict()
    for track in result:
        MSDID = track.pop('_id')
        lyrics[MSDID] = track
    return lyrics

def read_metadata_by_MSDID(MSDID_set):
    conn = sqlite3.connect('/mnt/snap/AdditionalFiles/track_metadata.db')
    q = '''SELECT track_id, 
                  title, 
                  artist_name,
                  artist_familiarity,
                  artist_hotttnesss,
                  year 
           FROM songs 
           WHERE year >= 1958;'''
    df = pd.read_sql_query(q, conn)
    mask = [(MSDID in MSDID_set) for MSDID in df['track_id'].values]
    df = df[mask]
    return df

def get_MSD_fields(MSDID):
    row = dict()
    h5 = hdf5.open_h5_file_read(f'/mnt/snap/data/{MSDID[2]}/{MSDID[3]}/{MSDID[4]}/{MSDID}.h5')

    row['msd_art_lat'] =  hdf5.get_artist_latitude(h5)
    row['msd_art_long'] =  hdf5.get_artist_longitude(h5)
    row['msd_loudness'] =  hdf5.get_loudness(h5)
    row['msd_energy'] =  hdf5.get_energy(h5)
    row['msd_danceability'] =  hdf5.get_danceability(h5)
    row['msd_duration'] =  hdf5.get_duration(h5)
    row['msd_key'] =  hdf5.get_key(h5)
    row['msd_key_conf'] = hdf5.get_key_confidence(h5)
    row['msd_mode'] = hdf5.get_mode(h5)
    row['msd_mode_conf'] = hdf5.get_mode_confidence(h5)
    row['msd_end_fadein'] = hdf5.get_end_of_fade_in(h5)
    row['msd_start_fadeout'] = hdf5.get_start_of_fade_out(h5)
    row['msd_song_hot'] = hdf5.get_song_hotttnesss(h5)
    h5.close()
    return row

def _reset_pgdb():
    """
    Deletes and recreates the tdcj SQL database.
    """
    conn = pg2.connect(host='localhost', port=5432, user='postgres')
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cur.execute('DROP DATABASE IF EXISTS billboard')
    cur.execute('CREATE DATABASE billboard')

    cur.close()
    conn.close()

def _create_tables():
    conn = pg2.connect(dbname='billboard', host='localhost', port=5432, user='postgres')
    cur = conn.cursor()

    command =\
    '''
    CREATE TABLE tracks(
        MSD_ID VARCHAR(18) PRIMARY KEY,
        MSD_track_title VARCHAR(64),
        MSD_artist_name VARCHAR(32),
        MSD_artist_familiarity NUMERIC,
        MSD_artist_hotttnesss NUMERIC,
        MSD_year SMALLINT,

        MSD_artist_latitude NUMERIC,
        MSD_artist_longitude NUMERIC,
        MSD_loudness NUMERIC,
        MSD_energy NUMERIC,
        MSD_danceability NUMERIC,
        MSD_duration INTEGER,
        MSD_key SMALLINT,
        MSD_key_confidence NUMERIC,
        MSD_mode SMALLINT,
        MSD_mode_confidence NUMERIC,
        MSD_end_of_fade_in INTEGER,
        MSD_start_of_fade_out INTEGER,
        MSD_song_hotttnesss NUMERIC,
        
        SPOT_track_name VARCHAR(64),
        SPOT_artist_name VARCHAR(32),
        SPOT_release_date SMALLINT,
        SPOT_explicit BOOLEAN,
        SPOT_danceability NUMERIC,
        SPOT_energy NUMERIC,
        SPOT_key SMALLINT,
        SPOT_loudness NUMERIC,
        SPOT_mode SMALLINT,
        SPOT_speechiness NUMERIC,
        SPOT_instrumentalness NUMERIC,
        SPOT_liveness NUMERIC,
        SPOT_valence NUMERIC,
        SPOT_tempo NUMERIC,
        SPOT_duration INTEGER,
        SPOT_time_signature SMALLINT,

        GEN_artist_name VARCHAR(32),
        GEN_track_title VARCHAR(64),
        GEN_lyrics TEXT,

        on_billboard BOOLEAN
    )
    '''
    cur.execute(command)
    conn.commit()
    cur.close()
    conn.close()

def insert_track(conn, cur, MSDID, af, lyrics, metadata, MSD_row):
    track_af = af[MSDID]
    track_lyrics = lyrics[MSDID]
    try:
        track_metadata = metadata[metadata['track_id']==MSDID].iloc[0]
    except IndexError:
        print(f'IndexError on df: {MSDID}', len(MSDID))
        return
    entry={
        **MSD_row,
        **track_lyrics,
        **track_af,
        'msd_id':track_metadata['track_id'],
        'msd_title':track_metadata['title'],
        'msd_art_name':track_metadata['artist_name'],
        'msd_art_fam':track_metadata['artist_familiarity'],
        'msd_art_hot':track_metadata['artist_hotttnesss'],
        'msd_year':int(track_metadata['year'])
    }
    entry['release_date'] = int(entry['release_date'][:4])
    entry['msd_key'] = int(entry['msd_key'])
    entry['msd_mode'] = int(entry['msd_mode'])
    entry['explicit'] = (entry['explicit']=='false')
    trim_dict(entry, ['msd_art_name', 'artist', 'response_artist'], 32)
    trim_dict(entry, ['msd_title', 'name', 'response_title'], 64)
    
    
    command = """ INSERT INTO tracks (
        MSD_ID,
        MSD_track_title,
        MSD_artist_name,
        MSD_artist_familiarity,
        MSD_artist_hotttnesss,
        MSD_year,

        MSD_artist_latitude,
        MSD_artist_longitude,
        MSD_loudness,
        MSD_energy,
        MSD_danceability,
        MSD_duration,
        MSD_key,
        MSD_key_confidence,
        MSD_mode,
        MSD_mode_confidence,
        MSD_end_of_fade_in,
        MSD_start_of_fade_out,
        MSD_song_hotttnesss,
        
        SPOT_track_name,
        SPOT_artist_name,
        SPOT_release_date,
        SPOT_explicit,
        SPOT_danceability,
        SPOT_energy,
        SPOT_key,
        SPOT_loudness,
        SPOT_mode,
        SPOT_speechiness,
        SPOT_instrumentalness,
        SPOT_liveness,
        SPOT_valence,
        SPOT_tempo,
        SPOT_duration,
        SPOT_time_signature,

        GEN_artist_name,
        GEN_track_title,
        GEN_lyrics,

        on_billboard
    )
    VALUES (
        %(msd_id)s,
        %(msd_title)s,
        %(msd_art_name)s,
        %(msd_art_fam)s,
        %(msd_art_hot)s,
        %(msd_year)s,

        %(msd_art_lat)s,
        %(msd_art_long)s,
        %(msd_loudness)s,
        %(msd_energy)s,
        %(msd_danceability)s,
        %(msd_duration)s,
        %(msd_key)s,
        %(msd_key_conf)s,
        %(msd_mode)s,
        %(msd_mode_conf)s,
        %(msd_end_fadein)s,
        %(msd_start_fadeout)s,
        %(msd_song_hot)s,
        
        %(name)s,
        %(artist)s,
        %(release_date)s,
        %(explicit)s,
        %(danceability)s,
        %(energy)s,
        %(key)s,
        %(loudness)s,
        %(mode)s,
        %(speechiness)s,
        %(instrumentalness)s,
        %(liveness)s,
        %(valence)s,
        %(tempo)s,
        %(duration_ms)s,
        %(time_signature)s,

        %(response_artist)s,
        %(response_title)s,
        %(lyrics)s,

        False
    );
    """
    try:
        cur.execute(command, entry)
    except StringDataRightTruncation as e:
        print(entry['msd_id'])
        print(entry)
        raise e

    conn.commit()

def trim_dict(d, keys, length):
    for key in keys:
        if len(d[key]) > length:
            d[key] = d[key][:length]

if __name__ == '__main__':
    if len(sys.argv) == 1:
        run_pipe()



