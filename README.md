# How to preprocess the data

1. Check that everything is set up correctly you should have a folder structure like this. Chunks can be found here: https://www.dropbox.com/sh/4jjl7yvipibfsj3/AAAK1j0sQPZNMEGlnjmw5MW8a?dl=0

```
.
+-- Finetuning
|   +-- src
|       +-- CHUNKS
|           +-- CHUNK1.csv
|           ...
|           +-- CHUNKX.csv
|       +-- data_cleaner.py
|       +-- main.py
|   +-- README.md
```

2. Install required packages with pip install requirements.txt

3. Run the main.py file which does the main filtering of the data

4. Run the data_cleaner.py file which does some after-processing after the filtering