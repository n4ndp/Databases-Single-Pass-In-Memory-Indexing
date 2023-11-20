import json
import pandas as pd
import ast
import sys
import os
import struct
import numpy as np
from collections import Counter
from spimi import SPIMI
from preprocessor import Preprocessor
from paths import DATA_DIR, BLOCKS_DIR

class IndexInverted:
    def __init__(self, file_name_data, number_of_dcouments, block_limit=200000, stop_words=True):
        """
        file_name_data: the name of the file containing the data (csv)
        number_of_dcouments: the number of documents in the data
        """
        self.file_name_data = file_name_data
        self.number_of_dcouments = number_of_dcouments
        self.block_limit = block_limit
        self.stop_words = stop_words

    def create_index_inverted(self):
        """Creates the inverted index and writes it to disk."""
        spimi = SPIMI(self.file_name_data, block_limit=self.block_limit, stop_words=self.stop_words).start() # Create the SPIMI object

        if spimi: # If SPIMI completed successfully
            return True
        else: # If SPIMI failed
            return False

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
                norm = round(np.sqrt(norm), 6) # Round to 6 decimal places

                id_encode = document_id.encode("utf-8") # Encode the document id
                norm_encode = struct.pack("f", norm)

                file_norms.write(id_encode)
                file_norms.write(norm_encode)

    def search_term(self, token):
        """Returns the postings list of a token using binary search."""
        with open(BLOCKS_DIR + "global_index.txt", "r") as file_global_index, open(BLOCKS_DIR + "metadata.bin", "rb") as file_metadata:
            low = 0
            high = file_metadata.seek(0, os.SEEK_END) // struct.calcsize("i") - 1 # Get the number of tokens in the file

            result = None

            while low <= high:
                mid = (low + high) // 2

                # struct.calcsize("i") is the size of an integer in bytes
                file_metadata.seek(mid * struct.calcsize("i")) # Seek to the middle of the file
                physical_position = struct.unpack("i", file_metadata.read(struct.calcsize("i")))[0] # Get the physical position of the token in the file

                file_global_index.seek(physical_position)
                line = file_global_index.readline() # Get the line at the current position in the file
                other_token = ast.literal_eval(line)[0] # Get the token at the current position

                if token < other_token:
                    high = mid - 1 # Search the left half
                elif token > other_token:
                    low = mid + 1 # Search the right half
                else:
                    postings_list = ast.literal_eval(line)[1] # Get the postings list
                    result = postings_list
                    break

            return result # Return the postings list of the token if found, otherwise None

    def search_norm(self, document_id):
        """Returns the norm of a document using binary search."""
        with open(DATA_DIR + "norms.bin", "rb") as file_norms:
            low = 0
            high = file_norms.seek(0, os.SEEK_END) // (len(document_id) + struct.calcsize("f")) - 1 # Get the number of documents in the file (self.number_of_dcouments - 1)

            result = None

            while low <= high:
                mid = (low + high) // 2

                # len(document_id) is the size of the document id in bytes
                # struct.calcsize("f") is the size of a float in bytes
                file_norms.seek(mid * (len(document_id) + struct.calcsize("f"))) # Seek to the middle of the file
                other_document_id = file_norms.read(len(document_id)).decode("utf-8") # Get the document id at the current position in the file

                if document_id < other_document_id:
                    high = mid - 1
                elif document_id > other_document_id:
                    low = mid + 1
                else:
                    norm = struct.unpack("f", file_norms.read(struct.calcsize("f")))[0] # Get the norm of the document
                    result = norm
                    break

            return result # Return the norm of the document if found, otherwise None
        
    def consult_query(self, query, topk):
        """Returns the top k documents for a given query."""
        scores = {} # Dictionary of document id to score

        query_preprocessed = [token for i, token in Preprocessor(None)._preprocess("query", query)] # Preprocess the query
        norm_query = 0
        for token, tf_query in Counter(query_preprocessed).items():
            postings_list = self.search_term(token)

            if postings_list: # If the token is in the index
                idf = np.log10(self.number_of_dcouments / len(postings_list)) # Calculate the idf of the token (universal for all documents)
                tf_query = np.log10(tf_query + 1) # Calculate the tf of the token in the query
                wt_query = tf_query * idf # Calculate the weight of the token in the query

                norm_query += np.square(wt_query)

                for document_id, tf in postings_list:
                    tf = np.log10(tf + 1) # Calculate the tf of the token in the document
                    wt = tf * idf # Calculate the weight of the token in the document

                    scores[document_id] = scores.get(document_id, 0) + wt_query * wt # Calculate the score of the document

        norm_query = np.sqrt(norm_query)

        for document_id, score in scores.items():
            norm_document = self.search_norm(document_id)

            if norm_query != 0 and norm_document != 0:
                scores[document_id] = score / (norm_query * norm_document) # Calculate the cosine similarity
            else:
                scores[document_id] = 0

        topk_documents = sorted(scores, key=scores.get, reverse=True)[:topk] # Get the top k documents

        return topk_documents # Return the top k documents for the query

if __name__ == "__main__":
    all_songs = pd.read_csv(DATA_DIR + "spotify_songs.csv")
    english_songs = all_songs[all_songs["language"] == "en"] # Only use English songs
    selected_columns = ["track_id", "track_name", "track_artist", "lyrics", "track_album_name", "playlist_name", "playlist_genre"] # Only use these columns
    filtered_english_songs = english_songs[selected_columns]
    filtered_english_songs = filtered_english_songs.head(100) # Only use the first 100 songs for testing
    filtered_english_songs_sorted = filtered_english_songs.sort_values(by='track_id')
    filtered_english_songs_sorted.to_csv(DATA_DIR + "spotify_songs_en.csv", index=False)

    file_name_data = "spotify_songs_en.csv"
    data_size = 100
    index_inverted = IndexInverted(file_name_data, data_size, block_limit=2000)
    #index_inverted.create_index_inverted()
    #index_inverted.write_norm_to_disk()
    ids = index_inverted.consult_query("The trees, are singing in the wind The sky blue", 1)
    print(ids)
