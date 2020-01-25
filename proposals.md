## Predicting a track's inclusion on the Billboard Top 100 chart.
### The bottom line:
I aim to make a model that will predict whether a track will be on the Billboard's Top 100 chart with 69% or greater accuracy and beat the nerds behind [the current top model](https://towardsdatascience.com/song-popularity-predictor-1ef69735e380). 

## The Minimum:
### My approach:
I expect to employ XGB for the competitive model and Random Forest and a Logistic Regressor for baselines. I plan to add a feature to test a hunch that it will increase prediction accuracy: emotional self-consistency, which is derived from a valence of a track and the lyrical sentiment.

### Interacting with my work
A well-made markdown file on this Githib repo.

### Data Source
I'll be using:
1. [Million Song Dataset](http://millionsongdataset.com/) 
2. lyrics scraped from [Genius](https://genius.com/) 
3. a sentiment lexicon (currently undecided but looking at [this one](https://juliasilge.github.io/tidytext/reference/sentiments.html))

## Moderately Ambitious 
Same as the mimimum, but with the following additions:
I'll incorporate the Spotify Data for songs in the years since 2012 (the million song dataset stops at 2012 I believe) using [spotifyr](https://github.com/charlie86/spotifyr) to fetch the data from the API. 

## Maximally Ambitious
Same as the moderate, but with the following additions:
I'll make a web app in which users will be able to search spotify for a song and my model will deliver the probability of it making the Billboard Top 100 along with the top 5 most affecting factors. 
  
