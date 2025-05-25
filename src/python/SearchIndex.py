from json import loads
from re import sub

index = dict(loads(open('index.json', 'r').read()))

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