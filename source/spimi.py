import json
import pandas as pd
import ast
import sys
import os
from preprocessor import Preprocessor
from paths import THIS_DIR, ROOT_DIR, DATA_DIR, BLOCKS_DIR

class SPIMI:
    def __init__(self, file_name, block_limit=100000):
        """
        file_name: the name of the file containing the data (not preprocessed)
        """
        self.file_name = file_name
        self.file_name_preprocessed = file_name[:-4] + ".json"
        print("Preprocessing the data...") # Preprocess the data
        preprocessor = Preprocessor(file_name, self.file_name_preprocessed, stop_words=True)
        preprocessor.preprocess()
        print("Preprocessing done.") # Preprocessing done
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
        
        print("Apllying the SPIMI algorithm...") # Apply the SPIMI algorithm
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
        print("SPIMI done.") # SPIMI done

        return block_number # Return the number of blocks created

    def list_blocks(self):
        """Returns a list of the blocks."""
        blocks = []
        for file in os.listdir(BLOCKS_DIR):
            if file.startswith('block'):
                blocks.append(file)

        return blocks # Return a list of the blocks created
    
    def list_global_index(self):
        """Returns a list of the global index."""
        global_index = []
        for file in os.listdir(BLOCKS_DIR):
            if file.startswith('local_index'):
                global_index.append(file)

        return global_index

    def merge_blocks(self, merged_block_number, block_names, other_block_names):
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
            if block_names == [] and other_block_names != []:
                # Rename all blocks in other_block_names to "local_index" + str(merged_block_number) + ".txt"
                for other_block_name in other_block_names:
                    os.rename(BLOCKS_DIR + other_block_name, BLOCKS_DIR + "local_index" + str(merged_block_number) + ".txt")
                    merged_block_list.append("local_index" + str(merged_block_number) + ".txt")
                    merged_block_number += 1

                return merged_block_number, merged_block_list
            
            if other_block_names == [] and block_names != []:
                # Rename all blocks in block_names to "local_index" + str(merged_block_number) + ".txt"
                for block_name in block_names:
                    os.rename(BLOCKS_DIR + block_name, BLOCKS_DIR + "local_index" + str(merged_block_number) + ".txt")
                    merged_block_list.append("local_index" + str(merged_block_number) + ".txt")
                    merged_block_number += 1

                return merged_block_number, merged_block_list
            
            else:
                return merged_block_number, []

        block_line = block.readline()
        other_block_line = other_block.readline()
        i, j = 0, 0 # index for block_names and other_block_names

        while True:
            if sys.getsizeof(merged_block) > self.block_limit:
                    merged_block_list.append(self.write_block_to_disk(merged_block, "local_index", merged_block_number, is_sorted=True))
                    merged_block_number += 1
                    merged_block = {}

            # CASE 1: block_names and other_block_names are not empty
            if block_line != "" and other_block_line != "":
                block_term_tuple = ast.literal_eval(block_line)
                other_block_term_tuple = ast.literal_eval(other_block_line)

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
                    # Delete the block file that was just read
                    os.remove(BLOCKS_DIR + block_names[i])
                    i += 1
                    if i < len(block_names):
                        block = open(BLOCKS_DIR + block_names[i], "r")
                        block_line = block.readline()

                if other_block_line == "":
                    other_block.close()
                    # Delete the block file that was just read
                    os.remove(BLOCKS_DIR + other_block_names[j])
                    j += 1
                    if j < len(other_block_names):
                        other_block = open(BLOCKS_DIR + other_block_names[j], "r")
                        other_block_line = other_block.readline()
            
            # CASE 2: block_line is empty
            elif block_line == "" and other_block_line != "":
                other_block_term_tuple = ast.literal_eval(other_block_line)
                merged_block[other_block_term_tuple[0]] = other_block_term_tuple[1]
                other_block_line = other_block.readline()

                if other_block_line == "":
                    other_block.close()
                    # Delete the block file that was just read
                    os.remove(BLOCKS_DIR + other_block_names[j])
                    j += 1
                    if j < len(other_block_names):
                        other_block = open(BLOCKS_DIR + other_block_names[j], "r")
                        other_block_line = other_block.readline()

            # CASE 3: other_block_line is empty
            elif block_line != "" and other_block_line == "":
                block_term_tuple = ast.literal_eval(block_line)
                merged_block[block_term_tuple[0]] = block_term_tuple[1]
                block_line = block.readline()

                if block_line == "":
                    block.close()
                    # Delete the block file that was just read
                    os.remove(BLOCKS_DIR + block_names[i])
                    i += 1
                    if i < len(block_names):
                        block = open(BLOCKS_DIR + block_names[i], "r")
                        block_line = block.readline()
            else:
                # CASE 4: block_names and other_block_names are empty
                break
        
        if merged_block:
            merged_block_list.append(self.write_block_to_disk(merged_block, "local_index", merged_block_number, is_sorted=True))
            merged_block_number += 1
        
        return merged_block_number, merged_block_list # Return the number of blocks created and the list of the blocks created

    def merge(self):
        """Merges all the blocks."""
        blocks = self.list_blocks()
        
        print("Merging the blocks for creating the global index...") # Merge the blocks for creating the global index
        # Merge the blocks in pairs
        controller = {int(block[5:-4]): [block] for block in blocks}
        merged_block_number = 0

        while len(controller) > 1:
            new_controller = {}
            i = 0

            # Merge blocks in pairs
            while i < len(controller):
                merged_block_number, merged_blocks_list = self.merge_blocks(merged_block_number, controller[i], controller[i + 1] if i + 1 < len(controller) else [])
                new_controller[i // 2] = merged_blocks_list
                i += 2

            # Update the controller for the next iteration
            controller = new_controller

        global_index = controller[0]
        print("Merging done.") # Merging done

        return global_index[0] # Return global index
    
    def start(self):
        """Starts the SPIMI algorithm."""
        self.spimi()
        self.merge()
        global_index = self.list_global_index() # List the global index, for example: ['local_index0.txt', 'local_index1.txt', 'local_index2.txt']
        
        # Create the global index file 
        with open(BLOCKS_DIR + "global_index.txt", 'w') as global_index_file:
            for local_index_filename in global_index:
                with open(BLOCKS_DIR + local_index_filename, 'r') as local_index_file:
                    content = local_index_file.read()
                    global_index_file.write(content)

        print("Global index created.") # Global index created
        
if __name__ == "__main__": # Example usage
    all_songs = pd.read_csv(DATA_DIR + "spotify_songs.csv")
    english_songs = all_songs[all_songs["language"] == "en"] # Only use English songs
    selected_columns = ["track_id", "track_name", "track_artist", "lyrics", "track_album_name", "playlist_name", "playlist_genre"] # Only use these columns
    filtered_english_songs = english_songs[selected_columns]
    test = filtered_english_songs.head(500) # Only use the first 500 songs for testing
    test.to_csv(DATA_DIR + "spotify_songs_en.csv", index=False)
    #filtered_english_songs.to_csv(DATA_DIR + "spotify_songs_en.csv", index=False)

    spimi = SPIMI("spotify_songs_en.csv", 20000)
    spimi.start()
