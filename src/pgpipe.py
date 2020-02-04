from pymongo import MongoClient
import sqlite3
import pd as pandas
import hdf5_getters as hdf5

class Postgres_Pipe:
    def __init__():
        self.mdb = MongoClient('localhost', 27017).tracks
        
    
    def run_pipe():
        mdb = MoncoClient('localhost', 27017).tracks

        af, MSDID_set = read_spotify_audio_features(tracks.audio_features)
        
        lyrics = read_genius_lyrics_by_MSDID(MSDID_set, tracks.lyrics)
        # TODO check/cut MSDID_set down

        metadata = read_metadata_by_MSDID(MSDID_set)
        # TODO check/cut MSDID_set down

        _reset_pgdb()
        _create_tables()

        for MSDID in MSDID_set:
            msd_row = get_MSD_fields(MSDID)
            insert_track(conn, cur, MSDID, af, lyrics, metadata, MSD_row)


    def read_spotify_audio_features(audio_feat_col):
        af = dict()
        MSDID_set = set()
        result = audio_feat_col.find({'_id':{'$exists':'true'}})
        af_keys_to_del = ['type', 'id', 'uri', 'track_href', 'analysis_url']
        meta_keys_to_keep = ['name', 'explicit', 'release_date']
        for track in result:
            MSDID = track['_id']
            MSDID_set.add(MSDID)
            for key in af_keys_to_del:
                del track['audio_features'][col]
            af[MSDID] = track['audio_features']
            for key in meta_keys_to_keep:
                af[MSDID][key] = track['metadata'][key]
            af[MSDID]['artist'] = track['artists']['name']
        return af, MSDID_set

    def read_genius_lyrics_by_MSDID(MSDID_set, lyrics_col):
        result = lyrics_col.find({'_id':{'$in': MSDID_set}})
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
        mask = [(MSDID in MSDID_set) for MSDID in set(df['track_id'].values)]
        df = df[mask]
        return df

    def get_MSD_fields(MSDID):
        row = [0]*13
        h5 = hdf5.open_h5_file_read(f'/mnt/snap/{MSDID[2]}/{MSDID[3]}/{MSDID[4]}/{MSDID}.h5')

        row['msd_art_lat'] =  h5.get_artist_latitude(h5)
        row['msd_art_long'] =  h5.get_artist_longitude(h5)
        row['msd_loudness'] =  h5.get_loudness(h5)
        row['msd_energy'] =  h5.get_energy(h5)
        row['msd_danceability'] =  h5.get_danceability(h5)
        row['msd_duration'] =  h5.get_duration(h5)
        row['msd_key'] =  h5.get_key(h5)
        row['msd_key_conf'] = h5.get_key_confidence(h5)
        row['msd_mode'] = h5.get_mode(h5)
        row['msd_mode_conf'] = h5.get_mode_confidence(h5)
        row['msd_end_fadein'] = h5.get_end_of_fade_in(h5)
        row['msd_start_fadeout'] = h5.get_start_of_fade_out(h5)
        row['msd_song_hot'] = h5.get_song_hotttnesss(h5)
        h5.close()
        return row

    def _reset_tdcj_pgdb():
        """
        Deletes and recreates the tdcj SQL database.
        """
        conn = pg2.connect(host='localhost', port=5432, user='postgres')
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        cur.execute('DROP DATABASE IF EXISTS tdcj')
        cur.execute('CREATE DATABASE billboard')

        cur.close()
        conn.close()

    def _create_tables():
        conn = pg2.connect(dbname='billboard', host='localhost', port=5432, user='postgres')
        cur = conn.cursor()

        command = 
        '''
        CREATE TABLE tracks(
            MSD_ID VARCHAR(18) PRIMARY KEY,
            MSD_track_title VARCHAR(30),
            MSD_artist_id VARCHAR(18),
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
            MSD_mode BOOLEAN,
            MSD_mode_confidence NUMERIC,
            MSD_end_of_fade_in INTEGER,
            MSD_start_of_fade_out INTEGER,
            MSD_song_hotttnesss NUMERIC,
            
            SPOT_track_name VARCHAR(30),
            SPOT_artist_name VARCHAR(30),
            SPOT_release_date SMALLINT,
            SPOT_explicit NUMERIC,
            SPOT_danceability NUMERIC,
            SPOT_energy NUMERIC,
            SPOT_key SMALLINT,
            SPOT_loudness NUMERIC,
            SPOT_mode BOOLEAN,
            SPOT_speechiness NUMERIC,
            SPOT_instrumentalness NUMERIC,
            SPOT_liveness NUMERIC,
            SPOT_valence NUMERIC,
            SPOT_tempo NUMERIC,
            SPOT_duration INTEGER,
            SPOT_time_signature SMALLINT,

            GEN_artist_name VARCHAR(30),
            GEN_track_title VARCHAR(30),
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
        track_metadata = metadata[metadata['track_id']==MSDID].iloc[0]
        entry={
            **MSD_row,
            **track_lyrics,
            **track_af,
            'msd_id':track_metadata['track_id'],
            'msd_title':track_metadata['title'],
            'msd_art_name':track_metadata['artist_name'],
            'msd_art_fam':track_metadata['artist_familiarity'],
            'msd_art_hot':track_metadata['artist_hotttnesss'],
            'msd_year':track_metadata['year']
        }
        entry['release_date'] = int(entry['release_date'][:4])

        command = """ INSERT INTO tracks (
            MSD_ID,
            MSD_track_title,
            MSD_artist_id,
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
            %(msd_conf)s,
            %(msd_mode)s,
            %(msd_mode_conf)s,
            %(msd_end_fadein)s,
            %(msd_fadeout)s,
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
        cur.execute(command, entry)
        conn.commit()
        
        



