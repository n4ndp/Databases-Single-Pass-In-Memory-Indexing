# **`Indexación en Memoria de un Solo Pase (SPIMI)`**

Implementación académica en Python de Single-Pass In-Memory Indexing (SPIMI).

## **Conjunto de Datos: [Audio features and lyrics of Spotify songs](https://www.kaggle.com/datasets/imuhammad/audio-features-and-lyrics-of-spotify-songs/)**

El conjunto de datos contiene información sobre más de 18,000 canciones de Spotify, incluyendo datos como artistas, álbumes, características de audio (como el volumen), letras, géneros, y más.

Ejemplo de datos:

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

## **Algoritmo SPIMI** 

El algoritmo SPIMI crea un índice invertido utilizando solo una pasada y un bloque de memoria. Funciona de la siguiente manera:

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

La combinación de bloques es similar al algoritmo [BSBI](https://nlp.stanford.edu/IR-book/html/htmledition/blocked-sort-based-indexing-1.html).
