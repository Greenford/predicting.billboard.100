from pymongo import MongoClient
import sqlite3
import pd as pandas
import hdf5_getters as hdf5

class Postgres_Pipe:
    def __init__():
        self.mdb = MongoClient('localhost', 27017).tracks
        
    
    def read_combine_by_MSDID():
        mdb = MoncoClient('localhost', 27017).tracks

        af, MSDID_set = read_spotify_audio_features(tracks.audio_features)
        
        lyrics = read_genius_lyrics_by_MSDID(MSDID_set, tracks.lyrics)
        # TODO check/cut MSDID_set down

        metadata = read_metadata_by_MSDID(MSDID_set)
        # TODO check/cut MSDID_set down

        MSD_analyses = read_MSD_analyses_by_MSDID(MSDID_set)
        # TODO check/cut MSDID_set down


        for MSDID in MSDID_set:
            pass
            # TODO match MSDIDs to billboard CSV

            # TODO compare artist names/track titles and cut below threshold

            # TODO put into PostgreSQL DB

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
        row = [0]*
        h5 = hdf5.open_h5_file_read(f'/mnt/snap/{MSDID[2]}/{MSDID[3]}/{MSDID[4]}/{MSDID}.h5')
        row[0] = h5.get_
        h5.close()

