# from nltk.sentiment import SentimentIntensityAnalyzer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# remove all nltk.download / ensure_vader logic entirely
# analyzer usage stays the same:
sia = SentimentIntensityAnalyzer()
