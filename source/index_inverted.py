import json
import pandas as pd
import ast
import sys
import os
import struct
import numpy as np
from spimi import SPIMI
from paths import DATA_DIR, BLOCKS_DIR

class IndexInverted:
    def __init__(self, file_name_data, number_of_dcouments):
        """
        file_name_data: the name of the file containing the data (csv)
        """
        self.file_name_data = file_name_data
        self.number_of_dcouments = number_of_dcouments

        spimi = SPIMI(file_name_data)
        if spimi.start()[0]:
            print("SPIMI completed successfully")
        else:
            print("SPIMI failed")

    def write_norm_to_disk(self):
        """Writes the norm of each document to disk."""
        norms = {} # Dictionary of document id to norm of document

        with open(BLOCKS_DIR + "global_index.txt", "r") as file_global_index:
            for line in file_global_index:
                postings_list = ast.literal_eval(line)[1] # Get the postings list

                idf = np.log10(self.number_of_dcouments / len(postings_list))

                for document_id, tf in postings_list:
                    tf = np.log10(tf + 1)
                    norms[document_id] = norms.get(document_id, 0) + (tf * idf) ** 2

        with open(DATA_DIR + "norms.bin", "wb") as file_norms:
            for document_id, norm in sorted(norms.items()):
                norm = round(np.sqrt(norm), 6)

                print("Writing norm of document {} to disk".format(document_id))

                id_encode = document_id.encode("utf-8") # Encode the document id
                norm_encode = struct.pack("f", norm)

                file_norms.write(id_encode)
                file_norms.write(norm_encode)

    def search_term(self, token):
        """Returns the postings list of a token using binary search."""
        with open(BLOCKS_DIR + "global_index.txt", "r") as file_global_index, open(BLOCKS_DIR + "metadata.txt", "r") as file_metadata:
            pass

if __name__ == "__main__":
    all_songs = pd.read_csv(DATA_DIR + "spotify_songs.csv")
    english_songs = all_songs[all_songs["language"] == "en"] # Only use English songs
    selected_columns = ["track_id", "track_name", "track_artist", "lyrics", "track_album_name", "playlist_name", "playlist_genre"] # Only use these columns
    filtered_english_songs = english_songs[selected_columns]
    filtered_english_songs_sorted = filtered_english_songs.sort_values(by='track_id')
    filtered_english_songs_sorted.to_csv(DATA_DIR + "spotify_songs_en.csv", index=False)

    file_name_data = "spotify_songs_en.csv"
    index_inverted = IndexInverted(file_name_data, 15405)
    index_inverted.write_norm_to_disk()
