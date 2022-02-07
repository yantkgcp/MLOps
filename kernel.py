import json


nb = json.load(open('pipeline.ipynb'))
nb['metadata']['kernelspec']['name'] = 'python3'
with open('pipeline.ipynb', 'w') as f:
    json.dump(nb, f)
    
nb = json.load(open('pipeline.ipynb'))
print(nb['metadata']['kernelspec']['name'])




