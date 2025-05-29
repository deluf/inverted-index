from os import listdir
from os.path import isfile
from re import compile
from sys import argv

if len(argv) < 3:
    exit(1)

dir = str(argv[1])
out = str(argv[2])

pattern = compile("[^A-Za-z]+")
index = dict()

for file in listdir(path=dir):

    if isfile(dir+file):

        with open(dir+file, "r") as f:

            for x in f:
                for word in x.split():
                    
                    word = pattern.sub('', word)

                    if len(word) == 0:
                        continue

                    word = word.lower()

                    if not word in index:
                        index.update([(word, dict())])

                    if file in index[word]:
                        index[word][file] += 1
                    else:
                        index[word].update([(file, 1)])

indexFile=open(out, "w")

for word, list in index.items():
    out = word
    for file, count in list.items():
        out += "\t" + file + ":" + str(count)
    print(out, file=indexFile)