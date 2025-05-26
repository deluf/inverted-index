from os import listdir
from os.path import isfile
from re import sub

index = dict()

for file in listdir(path='./IndexFiles'):
    
    if isfile("./IndexFiles/"+file):

        with open("./IndexFiles/"+file) as f:
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
                    if file in index[word]:
                        index[word][file] += int(val[-1])
                    else:
                        index[word].update([(val[0], int(val[-1]))]) 

result = set()
first = True
for word in input().split():
    
    word = sub('[^A-Za-z0-9\']+', '', word)

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