from os import listdir
from os.path import isfile
from re import compile
from sys import argv

# Input arguments: 
#  String: input file location (must be a directory path, ending with /, example: ./inputs/)
#  String: output file
if len(argv) < 3:
    exit(1)

dir = str(argv[1])
out = str(argv[2])

# Regex pattern pre-compile
pattern = compile("[^A-Za-z]+")

# Index data structure
index = dict()

# Loop through all the files in the input directory
for file in listdir(path=dir):

    if isfile(dir+file):

        with open(dir+file, "r") as f:

            for x in f:

                # Add each word reference to the index
                for word in x.split():
                    
                    # Word cleanup
                    word = pattern.sub('', word)

                    if len(word) == 0:
                        continue

                    word = word.lower()

                    # If the word is not in the index add it
                    if not word in index:
                        index.update([(word, dict())])

                    # Add the reference to the index:
                    if file in index[word]:
                        # If the file is already in the list of a given word, increase the count
                        index[word][file] += 1
                    else:
                        # If the file is not in the list of a given word, add it with count 1
                        index[word].update([(file, 1)])

# Output the index in with a line for each word with tab separeted pairs of all relative <filename>:<count> references
indexFile=open(out, "w")
for word, list in index.items():
    out = word
    for file, count in list.items():
        out += "\t" + file + ":" + str(count)
    print(out, file=indexFile)