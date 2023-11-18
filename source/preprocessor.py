import json
import pandas as pd
import os
import re
import nltk
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from paths import DATA_DIR

class Preprocessor:
    def __init__(self, file_name_data, stop_words=False):
        """
        file_name_data: the name of the file containing the data (csv)
        stop_words: a boolean indicating whether to remove stop words
        """
        self.file_name_data = file_name_data

        self.stop_words = set(stopwords.words("english")) if stop_words else None # Set of stop words
        self.word_tokenize = word_tokenize # Function for tokenizing words
        self.ps = PorterStemmer() # Stemmer

    def _preprocess(self, id, content):
        """Preprocess the text by tokenizing, removing stop words, and stemming."""
        tokens = [word.lower() for word in self.word_tokenize(content)] # Tokenize the content of the song and convert to lowercase

        if self.stop_words: # Remove stop words
            tokens = [word for word in tokens if word not in self.stop_words]

        tokens = [word for word in tokens if re.match(r'^[A-Za-z]+$', word)] # Remove non-alphabetic characters

        for token in tokens:
            token = self.ps.stem(token)
            yield (id, token) # Return a tuple of the id and the token

    def preprocess(self):
        """Preprocess the data"""
        data = pd.read_csv(DATA_DIR + self.file_name_data)

        for index, row in data.iterrows():
            track_name = row["track_name"]
            track_artist = row["track_artist"]
            lyrics = row["lyrics"]
            track_album_name = row["track_album_name"]
            playlist_name = row["playlist_name"]
            playlist_genre = row["playlist_genre"]

            content = track_name + " " + track_artist + " " + lyrics + " " + track_album_name + " " + playlist_name + " " + playlist_genre # Combine all the columns into one string
            track_id = row["track_id"] # The id of the song

            for tuple_id_token in self._preprocess(track_id, content):
                yield tuple_id_token # Return a tuple of the id and the token

    def token_stream(self):
        """Generates a stream of tokens"""
        for tuple_id_token in self.preprocess():
            yield tuple_id_token  # Emits each token tuple produced by preprocess()


if __name__ == "__main__": # Example usage
    all_songs = pd.read_csv(DATA_DIR + "spotify_songs.csv")
    english_songs = all_songs[all_songs["language"] == "en"] # Only use English songs
    selected_columns = ["track_id", "track_name", "track_artist", "lyrics", "track_album_name", "playlist_name", "playlist_genre"] # Only use these columns
    filtered_english_songs = english_songs[selected_columns]
    test = filtered_english_songs.head(3) # Only use the first 3 songs for testing
    test.to_csv(DATA_DIR + "spotify_songs_es_3.csv", index=False)

    preprocessor = Preprocessor("spotify_songs_es_3.csv", stop_words=True)

    # Iterate through the token stream and print tokens
    for track_id, token in preprocessor.token_stream():
        print(f"Track ID: {track_id}, Token: {token}")
