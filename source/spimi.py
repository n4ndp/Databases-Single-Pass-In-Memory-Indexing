import json
import pandas as pd
import ast
import sys
import os
import struct
from preprocessor import Preprocessor
from paths import DATA_DIR, BLOCKS_DIR

class SPIMI:
    def __init__(self, file_name_data, block_limit=200000):
        """
        file_name_data: the name of the file containing the data (csv)
        block_limit: the maximum size of a block in bytes
        """
        self.file_name_data = file_name_data
        self.block_limit = block_limit

    def write_block_to_disk(self, dictionary, block_name, block_number, is_sorted=False):
        """
        dictionary: the dictionary to be written to disk
        block_name: the name of the block to be written to disk
        block_number: the number of the block to be written to disk
        is_sorted: whether the dictionary is sorted or not
        """
        """Saves the block to a text file."""
        if not os.path.exists(BLOCKS_DIR):
            os.makedirs(BLOCKS_DIR)

        with open(BLOCKS_DIR + block_name + str(block_number) + '.txt', 'w') as file:
            if is_sorted:
                for term, postings_list in dictionary.items():
                    file.write(str((term, postings_list)) + '\n')
            else:
                for term, postings_list in sorted(dictionary.items()):
                    file.write(str((term, postings_list)) + '\n')

        return block_name + str(block_number) + '.txt' # Return the name of the block created

    def spimi(self):
        """Applies the Single-pass in-memory indexing algorithm to the preprocessed data."""
        block_number = 0
        block_list = []
        dictionary = {} # (term - postings list)

        preprocessor = Preprocessor(self.file_name_data, stop_words=True) # Preprocess the data

        for track_id, token in preprocessor.token_stream():
            if token not in dictionary:
                dictionary[token] = [(track_id, 1)] # Add the token to the dictionary with the track_id and the frequency
            else: # token already in dictionary
                postings_list = dictionary[token]

                if postings_list[-1][0] == track_id:
                    postings_list[-1] = (track_id, postings_list[-1][1] + 1) # Update the frequency
                else: # different track_id
                    postings_list.append((track_id, 1))

                dictionary[token] = postings_list # update postings list

            if sys.getsizeof(dictionary) > self.block_limit:
                block_list.append(self.write_block_to_disk(dictionary, "block", block_number)) # Write the block to disk
                block_number += 1 # Increment block number
                dictionary = {} # reset dictionary

        # Write the last block to disk
        if dictionary:
            block_list.append(self.write_block_to_disk(dictionary, "block", block_number))

        return block_list # Return the list of blocks created

    def merge_blocks(self, merged_block_number, block_names, other_block_names):
        """
        merged_block_number: the number of previously created blocks
        block_names: a list of the names of the blocks to be merged
        other_block_names: a list of the names of the other blocks to be merged
        """
        """Merges two blocks."""
        merged_block = {} # Merge the two blocks
        merged_block_list = [] # List of blocks created

        block = None
        other_block = None

        # Open the blocks to be merged
        if block_names and other_block_names: # Both lists are not empty
            block = open(BLOCKS_DIR + block_names[0], "r")
            other_block = open(BLOCKS_DIR + other_block_names[0], "r")

        elif block_names: # Only block_names is not empty
            # Rename all blocks in block_names to "local_index" + str(merged_block_number) + ".txt"
            for block_name in block_names:
                os.rename(BLOCKS_DIR + block_name, BLOCKS_DIR + "local_index" + str(merged_block_number) + ".txt")
                merged_block_list.append("local_index" + str(merged_block_number) + ".txt")
                merged_block_number += 1
            return merged_block_number, merged_block_list # Return the list of blocks "created"

        elif other_block_names: # Only other_block_names is not empty
            # Rename all blocks in other_block_names to "local_index" + str(merged_block_number) + ".txt"
            for block_name in other_block_names:
                os.rename(BLOCKS_DIR + block_name, BLOCKS_DIR + "local_index" + str(merged_block_number) + ".txt")
                merged_block_list.append("local_index" + str(merged_block_number) + ".txt")
                merged_block_number += 1
            return merged_block_number, merged_block_list # Return the list of blocks "created"

        else: # Both lists are empty
            return merged_block_number, merged_block_list

        # Merge the blocks
        block_line = block.readline()
        other_block_line = other_block.readline()
        i, j = 0, 0 # index for block_names and other_block_names

        while True:
            if sys.getsizeof(merged_block) > self.block_limit:
                    merged_block_list.append(self.write_block_to_disk(merged_block, "local_index", merged_block_number, is_sorted=True)) # Write the block to disk
                    merged_block_number += 1
                    merged_block = {}

            # CASE 1: block_names and other_block_names are not empty
            if block_line and other_block_line:
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
            elif other_block_line:
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
            elif block_line:
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
            merged_block_list.append(self.write_block_to_disk(merged_block, "local_index", merged_block_number, is_sorted=True)) # Write the block to disk
            merged_block_number += 1
        
        return merged_block_number, merged_block_list # Return the number of blocks created and the list of the blocks created

    def merge(self, spimi_blocks):
        """
        spimi_blocks: a list of the names of the blocks created by the spimi algorithm
        """
        """Merges all the blocks."""
        merged_block_number = 0
        controller = {int(block[5:-4]): [block] for block in spimi_blocks}

        # Merge the blocks in pairs
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
        return global_index # Return the global index

    def start(self):
        """Start the SPIMI algorithm and merges the blocks to obtain the global index."""
        token_number = 0 # Number of tokens in the data
        blocks = self.spimi() # Apply the SPIMI algorithm
        global_index = self.merge(blocks) # Merge the blocks
        
        with open(BLOCKS_DIR + "global_index.txt", 'w') as global_index_file, open(BLOCKS_DIR + "metadata.bin", 'wb') as metadata_file:
            physical_position = 0 # Physical position of the term in the global index
            for local_index_filename in global_index:
                with open(BLOCKS_DIR + local_index_filename, 'r') as local_index_file:
                    for line in local_index_file:
                        global_index_file.write(line)
                        token_number += 1 # Increment the number of tokens
                        metadata_file.write(struct.pack("i", physical_position)) # Write the physical position of the term in the global index
                        physical_position = global_index_file.tell() # Update the physical position

                os.remove(BLOCKS_DIR + local_index_filename) # Delete the local index file

        return True, token_number # Return True if the algorithm was successful

if __name__ == "__main__": # Example usage
    all_songs = pd.read_csv(DATA_DIR + "spotify_songs.csv")
    english_songs = all_songs[all_songs["language"] == "en"] # Only use English songs
    selected_columns = ["track_id", "track_name", "track_artist", "lyrics", "track_album_name", "playlist_name", "playlist_genre"] # Only use these columns
    filtered_english_songs = english_songs[selected_columns]
    filtered_english_songs.to_csv(DATA_DIR + "spotify_songs_en.csv", index=False)
    spimi = SPIMI("spotify_songs_en.csv")
    print(spimi.start())
