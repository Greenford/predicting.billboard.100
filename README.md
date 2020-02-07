# Predicting the BillBoard 100
### Background: 
The [Song Popularity Predictor(SPP)](https://towardsdatascience.com/song-popularity-predictor-1ef69735e380) was published in May 2018 and set the bar: a ROC AUC of 0.68 using an XGB classifier. 

After reading David Laing's [Analysis of Kendrick Lamar's Discography](https://davidklaing.com/blog/2017/05/07/kendrick-lamar-data-science.html) and seeing the graph that correllated Genius pageviews and emotional self-consistency, I wanted to test whether the emotional self-consistency of a song would help in predicting whether a song made the BillBoard 100. ... and I wanted to beat the SPP.

What is emotional self-consistency? Take a look at the image below from Laing's article. 

(Image)[]
Notice that most of the tracks on the album *good kid, m.A.A.d city* are more self-consistent than those of *To Pimp A ButterFly*. Another definition would be the absolute similarity between musical sentiment and lyrical sentiment. 

### Data Collection
[Million Song Dataset](http://millionsongdataset.com/) 
HDF5 files
BillBoard songs were in a CSV file from the SPP team.
[LyricsGenius](https://github.com/johnwmillr/LyricsGenius) is a Python library interface for the Genius API. Song lyrics were needed to compute the lyrical sentiment.
Similarly, [Spotipy](https://github.com/plamere/spotipy) is a Python library interface for the Spotify API. Spotify has a computed valence for many of their songs indicating the amount of negative or positive sentiment.

### Data Pipeline
Lyrics Mongo + sentiment preprocessing + Spotify Mongo + MSD + MSD Meta + CSV -> PostgreSQL DB

### Feature Engineering
Talk making emotional consistency and total sentiment, combining mode and mode confidence, NLP sentiment computation

### Predicting Modeling
Random Forest initial run
saw class imbalance was HUGE. Had to use a metric that didn't involve TP. 
XGB
