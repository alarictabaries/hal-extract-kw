import glob, json

abstracts = []

for documents_file in glob.glob('data/' + '*.json'):
    with open(documents_file) as f:
        data = json.load(f)
        for d in data:
            abstracts.append(d)

print(len(abstracts))