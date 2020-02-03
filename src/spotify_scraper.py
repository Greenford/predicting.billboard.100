import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

class Spotify_Scraper:
    def __init__(sleeptime):
        self.sleeptime = sleeptime
        client_credentials_manager = SpotifyClientCredentials()
        with open('data/spotify.auth', 'r') as file:
            client_id = file.read().strip()
            client_secret = file.read().strip()

        client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
        self.sp = spotipy.Spotipy(client_credentials_manager=client_credentials_manager)
        client = MongoClient('localhost', 27017)
        self.audio_features = client.tracks.audio_features
        self.audio_errlog = client.tracks.audio_errlog
        

        conn = sqlite3.connect('/mnt/snap/AdditionalFiles/track_metadata.db')
        q = '''SELECT track_id, title, artist_name, year FROM songs 
               WHERE year >= 1958 ORDER BY year DESC;'''
        self.df = pd.read_sql_query(q, conn)
        
        track_ids_with_lyrics = {track['_id'] for track in client.tracks.lyrics.find(
            {'_id':{'$exists':'true'}}, {'_id':'true'})}
        self.df = df[df['track_id'] in track_ids_with_lyrics]

    def get_spotify_URI(self, artist, trackname):
        result = self.sp.search(f'artist:{artist} track:{trackname}')
        if(result['tracks']['total'] == 0):
            return None
        else:
            return result['tracks']['items'][0]
    
    def get_audio_features_by_URIs(self, URIlist):
        return self.sp.audio_features(URIlist)

    def scrape_all(self):
        i = 0
        num_tracks self.df.shape[0]
        while i < num_tracks
            prev_i = i
            tracks_arr = []
            err_arr = []
            while (i - prev_i < 50) & (i < num_tracks):
                MSDID = row['track_id']
                row = self.df.iloc[i]
                trackdata = self.get_spotify_URI(row['artist_name'], row['title'])
                if trackdata == None:
                    err_arr.append({'_id': MSDID, 'msg': 'No Spotify data'})
                else:
                    track_arr.append({'_id':MSDID, 'metadata': trackdata})
                i += 1
                time.sleep(self.sleeptime)
            URIlist = list(map(lambda x: x['metadata']['uri'], tracks_arr))
            af_arr = self.sp.audio_features(URIlist)
            self.insert_to_mongo(tracks_arr, af_arr, err_arr)


    def insert_to_mongo(self, tracks_arr, af_arr, err_arr):
        af_arr = {features['uri']: features for features in af_arr}
        for track in tracks_arr:
            URI = track['metadata']['uri']
            track['audio_features'] = af_arr[URI]
        self.audio_features.insert_many(tracks_arr)
        self.audio_errlog.insert_many(err_arr)


            

