# How to preprocess the data

1. Check that everything is set up correctly you should have a folder structure like this

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

2. Run the main.py file which does the main filtering of the data

3. Run the data_cleaner.py file which does some after-processing after the filtering