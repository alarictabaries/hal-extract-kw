import matplotlib.pyplot as plt
import pymongo
import numpy as np

# Connect to MongoDB server
server = pymongo.MongoClient("mongodb://localhost:27017/")
db = server['hal']

col = db['docType']

interest = ['ART', 'COMM', 'THESE', 'MEM']
interest_sum = []

raw_match = 0
raw_match_80 = 0
raw_match_60 = 0

count = 0

for document in col.find({}):
    if document['lang'] == 'fr':
        count += document['count']
        raw_match += document['raw_match'] * document['count']
        raw_match_80 += document['raw_match_80'] * document['count']
        raw_match_60 += document['raw_match_60'] * document['count']

        if document['docType'] in interest :
            interest_sum.append({'docType': document['docType'], 'raw_match': document['raw_match']})

raw_match = raw_match / count
raw_match_80 = raw_match_80 / count
raw_match_60 = raw_match_60 / count

print(raw_match)

x = []
y = []

for i in interest_sum:
    x.append(i['docType'])
    y.append(i['raw_match'])

print(x)
print(y)

y_pos = np.arange(len(x))

fig, ax = plt.subplots()

# Create bars
plt.bar(y_pos, y, color=['mediumaquamarine', 'yellowgreen', 'cornflowerblue', 'blue', 'cyan'])

# Create names on the x-axis
plt.xticks(y_pos, x)

ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)

# Show graphic
plt.show()
