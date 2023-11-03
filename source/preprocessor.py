import json
import pandas as pd
import os
import nltk
nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.dirname(os.path.realpath(THIS_DIR))
DATA_DIR = ROOT_DIR + '/data/'

class Preprocessor:
    def __init__(self, file_name_data, file_name_preprocessed, stop_words=False):
        """
        file_name_data: the name of the file containing the data
        file_name_preprocessed: the name of the file to save the preprocessed data
        stop_words: a boolean indicating whether to remove stop words
        """
        self.file_name_data = file_name_data
        self.file_name_preprocessed = file_name_preprocessed

        self.stop_words = set(stopwords.words("english")) if stop_words else None
        self.word_tokenize = word_tokenize
        self.ps = PorterStemmer()

    def _preprocess(self, text):
        """Preprocess the text by tokenizing, removing stop words, and stemming."""
        tokens = [word.lower() for word in self.word_tokenize(text)]
        if self.stop_words: # Remove stop words
            tokens = [word for word in tokens if word not in self.stop_words]

        stoplist = ["0","1","2","3","4","5","6","7","8","9","0","_","--","\\","&",
                    "^",">",'.',"@","=","$",'?','[',']','¿',"(",")",'-','!',"<",
                    '\'',',',":","``","''",";","»",'(-)',"+","/","«","{","}",
                    "--","|","`","~"]
        
        tokens = [word for word in tokens if word not in stoplist]

        tokens = [self.ps.stem(word) for word in tokens]

        return tokens

    def _preprocess_data(self):
        """Preprocess the data and save it as a JSON file."""
        data = pd.read_csv(DATA_DIR + self.file_name_data)
        processed_data = [] # A list of dictionaries containing the processed data for each song

        for index, row in data.iterrows():
            track_id = row["track_id"]
            track_name = row["track_name"]
            track_artist = row["track_artist"]
            lyrics = row["lyrics"]
            track_album_name = row["track_album_name"]
            playlist_name = row["playlist_name"]
            playlist_genre = row["playlist_genre"]

            text = track_name + " " + track_artist + " " + lyrics + " " + track_album_name + " " + playlist_name + " " + playlist_genre # Combine all the text into one string
            tokens = self._preprocess(text)

            # Create a dictionary for each song
            song_data = {
                "track_id": track_id,
                "track_processed": tokens
            }

            processed_data.append(song_data)

        # Save the processed data as a JSON file in the data directory
        with open(DATA_DIR + self.file_name_preprocessed, 'w') as file:
            json.dump(processed_data, file)

    def preprocess(self):
        self._preprocess_data()

if __name__ == "__main__": # Example usage
    all_songs = pd.read_csv(DATA_DIR + "spotify_songs.csv")
    english_songs = all_songs[all_songs["language"] == "en"] # Only use English songs
    selected_columns = ["track_id", "track_name", "track_artist", "lyrics", "track_album_name", "playlist_name", "playlist_genre"] # Only use these columns
    filtered_english_songs = english_songs[selected_columns]
    test = filtered_english_songs.head(3) # Only use the first 3 songs for testing
    test.to_csv(DATA_DIR + "test.csv", index=False)

    preprocessor = Preprocessor("test.csv", "test.json", stop_words=True)
    preprocessor.preprocess()

    with open(DATA_DIR + "test.json") as file:
        processed_data = json.load(file)
        print(len(processed_data)) # Should be 3
