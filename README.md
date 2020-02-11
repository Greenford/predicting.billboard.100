# Predicting the BillBoard 100
### Background: 
The [Song Popularity Predictor (SPP)](https://towardsdatascience.com/song-popularity-predictor-1ef69735e380) was published in May 2018 and set what seemed like an easy bar to beat: a ROC AUC of 68% using an XGB classifier. An [Analysis of Kendrick Lamar's Discography](https://davidklaing.com/blog/2017/05/07/kendrick-lamar-data-science.html) by David Laing had also caught my eye with data that correllated pageviews on Genius (a lyrics website) and emotional self-consistency of a track. I was curious whether that correlation would extend to a larger dataset and to the popularity of tracks as measured by the BillBoard Hot 100, so I set out to test it... and to beat the SPP.


What is emotional self-consistency? Take a look at the image below from Laing's article. 

(Image)[]
Notice that most of the tracks on the album *good kid, m.A.A.d city* are more self-consistent than those of *To Pimp A ButterFly*. Another definition would be the absolute difference between musical sentiment and lyrical sentiment. 
Following Laing's article, I used the 'valence' field that Spotify computes for many of their songs as musical sentiment, and did sentiment analysis on Genius lyrics using the Bing lexicon to compute a lyrical sentiment for each song. 

A note on terms: in this project, I reserve the use of the word 'song' to mean the lyrics, score, etc... everything to do with a piece of music that can exist without being recorded or performed. I hardly use the word, as my project studies only 'tracks', or the recordings of songs that can be listened to. 


### Data Collection

My first 3 data sources are the same as the in the SPP, although my use of the Spotify data differed - more details below. Genius was the 4th source. 
1. [Million Song Dataset (MSD)](http://millionsongdataset.com/) 
This is a 280 gigabyte public dataset compiled by Echonest, which was acquired by Spotify in 2014. The million *tracks* are mostly western popular music recorded anytime between the early 1900s and 2011, although I only used track data from 1958 onward(the start of BillBoard). It's stored in HDF5 files which were typically used for heavy scientific computing, but I'm not sure if it's still the ideal technology. It's worth noting that pulling anything other than numeric data is very slow (strings, for example), and that a full file open/close cycle is required for the data of every track fetched because of the storage structure of the mass of the MSD. Because of this a track metadata file was provided in SQLite format, and it's ideal to pull strings such as artist name and track title from there, and then match those to the HDF5 files using the track_id to get the numeric features. 

2. A list of about 20,000 BillBoard 100 tracks were in a CSV file in the SPP Github repo, and included tracks from 1958-2012.

3. Spotify data was gathered with [Spotipy](https://github.com/plamere/spotipy), a Python library written to interface with the Spotify API. Spotify's most important feature was valence for this project, although they also have fields such as acousticness, danceability, energy, loudness, liveness, speechiness, and instrumentalness... truely a wealth of music data. 

4. Like Spotify, Genius data was pulled with [LyricsGenius](https://github.com/johnwmillr/LyricsGenius), a Python library for interfacing with the Genius API. This data was most important because the lyrics were needed to compute the lyrical sentiment, though other data is available. 

### Data Pipeline
The artist name and track title were taken from the MSD and used to search through the sources, and all the data was connected using the MSD track id. I managed to gather mostly complete data for about 190,000 tracks (20% of the MSD compared to the 1% subset the SPP used). Otherwise the flow followed the below diagram. 
[image of flowchart]()
Sentiment scores were calculated using a separately threaded process that fetched the lyric texts from the MongoDB, calculated the sentiments, and stored the scores back into the respective documents. This was faster than calculating the lyrical sentiment from scratch everytime it was needed. 

### Feature Engineering
After reading the SPP article I wanted to prioritize scraping complete data, so I started with MSD entries that had year data (about 512,000) or had a match in the BillBoard list (about 2800). Missing 
Talk making emotional consistency and total sentiment, combining mode and mode confidence, NLP sentiment computation

### Predicting Modeling
Random Forest initial run
saw class imbalance was HUGE. Had to use a metric that didn't involve TP. 
XGB
