import json
import sys
import os

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.dirname(os.path.realpath(THIS_DIR))
DATA_DIR = ROOT_DIR + '/data/'
BLOCKS_DIR = DATA_DIR + '/blocks/'

class SPIMI:
    def __init__(self, file_name_preprocessed, block_limit=10000):
        """
        file_name_preprocessed: the name of the file containing the preprocessed data
        """
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

if __name__ == "__main__": # Example usage
    spimi = SPIMI("test.json", 2000)
    print(spimi.spimi())