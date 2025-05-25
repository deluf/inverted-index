from os import listdir
from os.path import isfile
from json import dumps
from re import sub

index = dict()

for file in listdir(path='./TestData'):
    
    if isfile("./TestData/"+file):

        with open("./TestData/"+file) as f:
            for x in f:
                for word in x.split():
                    
                    word = sub('[^A-Za-z0-9\']+', '', word)

                    if len(word) == 0:
                        continue

                    word = word.lower()

                    if not word in index:
                        index.update([(word, dict())])

                    if file in index[word]:
                        index[word][file] += 1
                    else:
                        index[word].update([(file, 1)])

print(dumps(index), file=open("index.json", "w"))