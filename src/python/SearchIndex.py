from json import loads

index = dict(loads(open('index.json', 'r').read()))

result = set()
first = True
for word in input().split():
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