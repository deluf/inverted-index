from os import listdir
from os.path import isfile
from re import compile
from sys import argv

if len(argv) < 2:
    exit(1)

dir = str(argv[1])

pattern = compile("[^A-Za-z]+")
index = dict()


def handleFile(f):
    for x in f:

        word = ""
        for val in x.split("\t"):

            if word == "":
                word = val
                if not word in index:
                    index.update([(word, dict())])
                continue
            
            val = val.strip("\n")
            val = val.split(":")
            if val[0] in index[word]:
                index[word][val[0]] += int(val[-1])
            else:
                index[word].update([(val[0], int(val[-1]))]) 

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
for word in input().split():
    
    word = pattern.sub('', word)

    if len(word) == 0:
        continue

    word = word.lower()

    if not word in index:
        result.clear()
        break

    if first:
        result = set(index[word].keys())
        first = False
    else:
        result = result.intersection(index[word].keys())
    
    if len(result) == 0:
        break

for r in result:
    print(r)