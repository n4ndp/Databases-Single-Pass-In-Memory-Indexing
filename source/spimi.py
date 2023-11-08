import json
import pandas as pd
import sys
import os
from preprocessor import Preprocessor

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.dirname(os.path.realpath(THIS_DIR))
DATA_DIR = ROOT_DIR + '/data/'
BLOCKS_DIR = DATA_DIR + '/blocks/'

class SPIMI:
    def __init__(self, file_name, file_name_preprocessed, block_limit=10000):
        """
        file_name: the name of the file containing the data (not preprocessed)
        """
        self.file_name = file_name
        preprocessor = Preprocessor(file_name, file_name_preprocessed, stop_words=True)
        preprocessor.preprocess()
        self.file_name_preprocessed = file_name_preprocessed
        self.block_limit = block_limit

    def load_preprocessed_data(self):
        """Load the preprocessed data from the JSON file."""
        with open(DATA_DIR + self.file_name_preprocessed) as file:
            data = json.load(file)
        return data
    
    def write_block_to_disk(self, dictionary, block_name, block_number, is_sorted=False):
        """Saves the block to a text file."""
        if not os.path.exists(BLOCKS_DIR):
            os.makedirs(BLOCKS_DIR)

        with open(BLOCKS_DIR + block_name + str(block_number) + '.txt', 'w') as file:
            if is_sorted:
                for term in dictionary.keys():
                    file.write(str((term, dictionary[term])) + '\n')
            else:
                for term in sorted(dictionary.keys()):
                    file.write(str((term, dictionary[term])) + '\n')

        return block_name + str(block_number) + '.txt' # Return the name of the block created

    def spimi(self):
        """Applies the Single-pass in-memory indexing algorithm to the preprocessed data."""
        data = self.load_preprocessed_data()
        # json file with songs preprocessed
        """
        [{"track_id": "1", "track_processed": ["Shape", "of", "You", "Ed", "Sheeran", "The", "club", "isn't", "the", "best", "place", "to", "find", "a", "lover", "So", "the", "bar", "is", "where", "I", "go"]}, 
        {"track_id": "2", "track_processed": ["Someone", "Like", "You", "Adele", "I", "heard", "that", "you're", "settled", "down", "That", "you", "found", "a", "girl", "and", "you're", "married", "now"]},
        {"track_id": "3", "track_processed": ["Uptown", "Funk", "Mark", "Ronson", "ft.", "Bruno", "Mars", "This", "hit,", "that", "ice", "cold", "Michelle", "Pfeiffer,", "that", "white", "gold"]}]
        """
        # blockX.txt with X = 0, 1, 2, ...
        """
        ("the",[(1,2)])
        ("that",[(2,2), (3,2)])
        """

        block_number = 0
        dictionary = {} # (term - postings list)

        for song in data:
            track_id = song["track_id"]
            tokens = song["track_processed"]
        
            for token in tokens:
                if token not in dictionary:
                    dictionary[token] = [(track_id, 1)]
                else: # token already in dictionary
                    postings_list = dictionary[token]

                    if postings_list[-1][0] == track_id:
                        postings_list[-1] = (track_id, postings_list[-1][1] + 1)
                    else: # different track_id
                        postings_list.append((track_id, 1))
                    
                    dictionary[token] = postings_list # update postings list

                if sys.getsizeof(dictionary) > self.block_limit:
                    self.write_block_to_disk(dictionary, "block", block_number)
                    block_number += 1
                    dictionary = {} # reset dictionary

        # Write the last block to disk
        if dictionary:
            self.write_block_to_disk(dictionary, "block", block_number)
            block_number += 1

        return block_number # Return the number of blocks created

    def list_blocks(self):
        """Returns a list of the blocks."""
        blocks = []
        for file in os.listdir(BLOCKS_DIR):
            if file.startswith('block'):
                blocks.append(file)

        return blocks # Return a list of the blocks created

    def merge_blocks(self, block_names, other_block_names, merged_block_number):
        """
        block_names: a list of the names of the blocks to be merged
        other_block_names: a list of the names of the other blocks to be merged
        """
        """Merges two blocks."""
        merged_block = {} # Merge the two blocks
        merged_block_list = []

        block = None
        other_block = None

        if block_names and other_block_names:
            block = open(BLOCKS_DIR + block_names[0], "r")
            other_block = open(BLOCKS_DIR + other_block_names[0], "r")
        else:
            return # ...

        block_line = block.readline()
        other_block_line = other_block.readline()
        i, j = 0, 0 # index for block_names and other_block_names

        while True:
            if sys.getsizeof(merged_block) > self.block_limit:
                    merged_block_list.append(self.write_block_to_disk(merged_block, "block_", merged_block_number, is_sorted=True))
                    merged_block_number += 1
                    merged_block = {}

            # CASE 1: block_names and other_block_names are not empty
            if block_line != "" and other_block_line != "":
                block_term_tuple = eval(block_line)
                other_block_term_tuple = eval(other_block_line)

                if block_term_tuple[0] == other_block_term_tuple[0]:
                    merged_postings_list = block_term_tuple[1] + other_block_term_tuple[1]
                    merged_block[block_term_tuple[0]] = merged_postings_list
                    block_line = block.readline()
                    other_block_line = other_block.readline()
                elif block_term_tuple[0] < other_block_term_tuple[0]:
                    merged_block[block_term_tuple[0]] = block_term_tuple[1]
                    block_line = block.readline()
                else:
                    merged_block[other_block_term_tuple[0]] = other_block_term_tuple[1]
                    other_block_line = other_block.readline()
                
                if block_line == "":
                    block.close()
                    i += 1
                    if i < len(block_names):
                        block = open(BLOCKS_DIR + block_names[i], "r")
                        block_line = block.readline()

                if other_block_line == "":
                    other_block.close()
                    j += 1
                    if j < len(other_block_names):
                        other_block = open(BLOCKS_DIR + other_block_names[j], "r")
                        other_block_line = other_block.readline()
            
            # CASE 2: block_line is empty
            elif block_line == "" and other_block_line != "":
                other_block_term_tuple = eval(other_block_line)
                merged_block[other_block_term_tuple[0]] = other_block_term_tuple[1]
                other_block_line = other_block.readline()

                if other_block_line == "":
                    other_block.close()
                    j += 1
                    if j < len(other_block_names):
                        other_block = open(BLOCKS_DIR + other_block_names[j], "r")
                        other_block_line = other_block.readline()

            # CASE 3: other_block_line is empty
            elif block_line != "" and other_block_line == "":
                block_term_tuple = eval(block_line)
                merged_block[block_term_tuple[0]] = block_term_tuple[1]
                block_line = block.readline()

                if block_line == "":
                    block.close()
                    i += 1
                    if i < len(block_names):
                        block = open(BLOCKS_DIR + block_names[i], "r")
                        block_line = block.readline()
            else:
                # CASE 4: block_names and other_block_names are empty
                break
        
        if merged_block:
            merged_block_list.append(self.write_block_to_disk(merged_block, "block_", merged_block_number, is_sorted=True))
            merged_block_number += 1
        
        return merged_block_number, merged_block_list # Return the number of blocks created and the list of the blocks created

    def merge(self):
        """Merges all the blocks."""
        
if __name__ == "__main__": # Example usage
    all_songs = pd.read_csv(DATA_DIR + "spotify_songs.csv")
    english_songs = all_songs[all_songs["language"] == "en"] # Only use English songs
    selected_columns = ["track_id", "track_name", "track_artist", "lyrics", "track_album_name", "playlist_name", "playlist_genre"] # Only use these columns
    filtered_english_songs = english_songs[selected_columns]
    test = filtered_english_songs.head(10) # Only use the first 10 songs for testing
    test.to_csv(DATA_DIR + "spotify_songs_en_test.csv", index=False)

    spimi = SPIMI("spotify_songs_en_test.csv", "spotify_songs_en_test.json", 10000)
    print(spimi.spimi())
    print(spimi.list_blocks())
    #print(spimi.merge_blocks(["block0test.txt", "block2test.txt"], ["block1test.txt", "block3test.txt"], 0))
