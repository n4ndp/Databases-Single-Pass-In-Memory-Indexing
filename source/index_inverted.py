import json
import pandas as pd
import ast
import sys
import os
from spimi import SPIMI

class IndexInverted:
    def __init__(self, file_name_data):
        """
        file_name_data: the name of the file containing the data (csv)
        """
        self.file_name_data = file_name_data

        spimi = SPIMI(file_name_data)
        if spimi.start():
            print("SPIMI completed successfully")
        else:
            print("SPIMI failed")


if __name__ == "__main__":
    file_name_data = "spotify_songs_en.csv"
    index_inverted = IndexInverted(file_name_data)
