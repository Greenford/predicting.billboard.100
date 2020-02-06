import nltk
nltk.download('opinion_lexicon')
from nltk.corpus import opinion_lexicon

class Sentimenter:
    def __init__:
        self.pos = set(opinion_lexicon.positive())
        self.neg = set(opinion_lexicon.negative())



