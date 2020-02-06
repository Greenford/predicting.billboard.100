import nltk
nltk.download('opinion_lexicon')
from nltk.corpus import opinion_lexicon
from nltk.tokenize import treebank
class Sentimenter:
    def __init__(self):
        self.pos = set(opinion_lexicon.positive())
        self.neg = set(opinion_lexicon.negative())
        self.tok = treebank.TreebankWordTokenizer()


    def sentiment(self, text):
        pcount = ncount = 0
        words = [word.lower() for word in self.tok.tokenize(text)]
        for word in words:
            if word in self.pos:
                pcount += 1
            elif word in self.neg:
                ncount += 1
        return (pcount-ncount)/(pcount+ncount+1)
    


