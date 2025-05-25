from os import listdir
from os.path import isfile
from json import dumps

index = dict()

for file in listdir(path='./TestData'):
    
    if isfile("./TestData/"+file):

        with open("./TestData/"+file) as f:
            for x in f:
                for word in x.split():
                    
                    if not word in index:
                        index.update([(word, dict())])

                    if file in index[word]:
                        index[word][file] += 1
                    else:
                        index[word].update([(file, 1)])

print(dumps(index), file=open("index.json", "w"))