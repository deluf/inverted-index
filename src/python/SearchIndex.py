from os import listdir
from os.path import isfile
from re import compile
from sys import argv

# Input argument: 
#  String: index location (can be either a file or a folder containing multiple index files)
if len(argv) < 2:
    exit(1)

dir = str(argv[1])

# Regex pattern pre-compile
pattern = compile("[^A-Za-z]+")

# Index data structure
index = dict()

# Function to parse an index file and add its contents to the index data structure
def handleFile(f):
    # Loop through the file lines
    for x in f:

        # Loop thorugh all the tab separated values of each line
        word = ""
        for val in x.split("\t"):

            # The first value is always the word
            if word == "":
                word = val
                if not word in index:
                    index.update([(word, dict())])
                continue
            
            # Subsequent values are in the format <filename>:<count>
            val = val.strip("\n")
            val = val.split(":")
            # Add the values to the index
            if val[0] in index[word]:
                # If using multiple index files it's possible to have different counts of a word for the same filename
                # We decided to add them together
                index[word][val[0]] += int(val[-1])
            else:
                index[word].update([(val[0], int(val[-1]))]) 

# Parse index file/directory
if isfile(dir):
    with open(dir, "r") as f:
        handleFile(f)
else:
    for file in listdir(path=dir):
        if isfile(dir+file):
            with open(dir+file) as f:
                handleFile(f)


result = set()
first = True
# Take from stdin the words to search for
for word in input().split():
    
    # Cleanup the words in the same way the index generator does
    word = pattern.sub('', word)

    if len(word) == 0:
        continue

    word = word.lower()

    # If a word is not in the index the final result should be empty, we can stop the search early
    if not word in index:
        result.clear()
        break

    if first:
        # The first word provides a list of possible result filenames
        result = set(index[word].keys())
        first = False
    else:
        # All other words can remove possible result filenames
        result = result.intersection(index[word].keys())
    
    # If there are no more possible results we can stop early
    if len(result) == 0:
        break

# Result output
for r in result:
    print(r)