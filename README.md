# **`Single-Pass-In-Memory-Indexing`**

This is a Python implementation of Single-Pass In-Memory Indexing (SPIMI).

## **Dataset: [Audio Features and Lyrics of Spotify Songs](https://www.kaggle.com/datasets/imuhammad/audio-features-and-lyrics-of-spotify-songs/)**

The dataset provides a comprehensive collection of data for more than 18,000 Spotify songs, including information about artists, albums, audio features (e.g., loudness), lyrics, language, genres, and sub-genres.

Here is an example of the data contained in the dataset:

```json
"track_id": "004s3t0ONYlzxII9PLgU6z",
"track_name": "I Feel Alive",
"track_artist": "Steady Rollin",
"lyrics": "The trees are singing in the wind. The sky is blue...",
"track_popularity": 28,
"track_album_id": "3z04Lb9Dsilqw68SHt6jLB",
"track_album_name": "Love & Loss",
...
"acousticness": 0.0117,
"instrumentalness": 0.00994,
"liveness": 0.347,
"valence": 0.404,
"tempo": 135.225,
"duration_ms": 373512,
"language": "en"
```

This dataset contains a wide range of information about Spotify songs, making it a valuable resource for various data analysis and research purposes. The data includes audio features, lyrics, and other metadata related to each song.

## **[SPIMI](https://nlp.stanford.edu/IR-book/html/htmledition/single-pass-in-memory-indexing-1.html) Algorithm**

The SPIMI algorithm is a single-pass algorithm for creating inverted indexes from a document collection. The algorithm is based on the observation that the postings lists for a given term are ordered in the document collection. The algorithm uses a single block of main memory to build the inverted index. The algorithm is as follows:

```
SPIMI-Invert(token_stream):
    output_file = new_file()
    dictionary = new_hash()
    while (free memory available)
    do token ← next(token_stream)
        if term(token) ∉ dictionary
            then postings_list = add_to_dictionary(dictionary, term(token))
            else postings_list = get_postings_list(dictionary, term(token))
        if full(postings_list)
            then postings_list = double_postings_list(dictionary, term(token))
        add_to_postings_list(postings_list, docID(token))
    sorted_terms ← sort_terms(dictionary)
    write_block_to_disk(sorted_terms, dictionary, output_file)
    return output_file
```

Merging of blocks is analogous to [BSBI](https://nlp.stanford.edu/IR-book/html/htmledition/blocked-sort-based-indexing-1.html) algorithm.

