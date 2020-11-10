import pymongo
from libraries import nlprocessing

# Connect to MongoDB server
server = pymongo.MongoClient("mongodb://localhost:27017/")
db = server['hal']
col = db['documents']

# Initialise some variables
count_fulfill = 0
domains = []
types = []
yearlies = []
# Set to false if in production
debug = True

unknown_words = [{'lang': 'fr', 'words': []}, {'lang': 'en', 'words': []}]

# Update with your stopwords
stopwords_homemade = [':', ',', ';', '.', '!', '?', ';', 'le', 'un', 'se','il','ce']

# Select all documents with interesting fields in english
cursor = col.find({"$and":[ {"en_abstract_s":{"$exists": True}}, {"en_keyword_s":{"$exists": True}}]}, no_cursor_timeout=True).batch_size(100)
for document in cursor:
    if debug:
        print('Current document :', end=' ')
        print(document)

    count_fulfill += 1
    abstract = document['en_abstract_s'][0]
    keywords = document['en_keyword_s']

    # Compute similarity between abstract and keywords
    similarity = nlprocessing.computeSimilarity(abstract, keywords, 'en', debug)

    for lang in unknown_words:
        if lang['lang'] == 'en':
            lang['words'].extend(similarity['words_unknown'])
            lang['words'].extend(similarity['keywords_unknown'])

    # Update domain's statistics
    domain_informed = False
    for domain in domains:
        if domain['primaryDomain'] == document['primaryDomain_s'] and domain['lang'] == 'en':
            domain_informed = True
            domain['count_fulfill'] += 1
            domain['avg_similarity'] += similarity['avg_similarity']
            domain['count_valid100'] += similarity['count_valid100_tmp']
            domain['count_valid80'] += similarity['count_valid80_tmp']
            domain['count_valid60'] += similarity['count_valid60_tmp']
            domain['avg_keywords_unknown'] += similarity['avg_keywords_unknown']
            domain['avg_words_unknown'] += similarity['avg_words_unknown']

    if not domain_informed:
        domains.append({'primaryDomain': document['primaryDomain_s'], 'lang': 'en', 'count_fulfill': 1, 'count_valid100': similarity['count_valid100_tmp'], 'count_valid80': similarity['count_valid80_tmp'], 'count_valid60': similarity['count_valid60_tmp'], 'avg_similarity': similarity['avg_similarity'], 'avg_words_unknown': similarity['avg_words_unknown'], 'avg_keywords_unknown': similarity['avg_keywords_unknown']})

    # Update type's statistics
    type_informed = False
    for type in types:
        if type['docType'] == document['docType_s'] and type['lang'] == 'en':
            type_informed = True
            type['count_fulfill'] += 1
            type['avg_similarity'] += similarity['avg_similarity']
            type['count_valid100'] += similarity['count_valid100_tmp']
            type['count_valid80'] += similarity['count_valid80_tmp']
            type['count_valid60'] += similarity['count_valid60_tmp']
            type['avg_keywords_unknown'] += similarity['avg_keywords_unknown']
            type['avg_words_unknown'] += similarity['avg_words_unknown']

    if not type_informed:
        types.append({'docType': document['docType_s'], 'lang': 'en', 'count_fulfill': 1, 'count_valid100': similarity['count_valid100_tmp'], 'count_valid80': similarity['count_valid80_tmp'], 'count_valid60': similarity['count_valid60_tmp'], 'avg_similarity': similarity['avg_similarity'], 'avg_words_unknown': similarity['avg_words_unknown'], 'avg_keywords_unknown': similarity['avg_keywords_unknown']})

    # Update yearly statistics
    yearly_informed = False
    for yearly in yearlies:
        if yearly['publicationDate'] == document['publicationDateY_i'] and yearly['lang'] == 'en':
            yearly_informed = True
            yearly['count_fulfill'] += 1
            yearly['avg_similarity'] += similarity['avg_similarity']
            yearly['count_valid100'] += similarity['count_valid100_tmp']
            yearly['count_valid80'] += similarity['count_valid80_tmp']
            yearly['count_valid60'] += similarity['count_valid60_tmp']
            yearly['avg_keywords_unknown'] += similarity['avg_keywords_unknown']
            yearly['avg_words_unknown'] += similarity['avg_words_unknown']

    if not yearly_informed:
        yearlies.append({'publicationDate': document['publicationDateY_i'], 'lang': 'en', 'count_fulfill': 1, 'count_valid100': similarity['count_valid100_tmp'], 'count_valid80': similarity['count_valid80_tmp'], 'count_valid60': similarity['count_valid60_tmp'], 'avg_similarity': similarity['avg_similarity'], 'avg_words_unknown': similarity['avg_words_unknown'], 'avg_keywords_unknown': similarity['avg_keywords_unknown']})

cursor.close()

print('Info % All english documents processed')

# Select all documents with interesting fields in french
cursor = col.find({"$and":[ {"fr_abstract_s":{"$exists": True}}, {"fr_keyword_s":{"$exists": True}}]}, no_cursor_timeout=True).batch_size(100)
for document in cursor:
    if debug:
        print('Current document :', end=' ')
        print(document)

    count_fulfill += 1
    abstract = document['fr_abstract_s'][0]
    keywords = document['fr_keyword_s']

    # Compute similarity between abstract and keywords
    similarity = nlprocessing.computeSimilarity(abstract, keywords, 'fr', debug)

    for lang in unknown_words:
        if lang['lang'] == 'fr':
            lang['words'].extend(similarity['words_unknown'])
            lang['words'].extend(similarity['keywords_unknown'])

    # Update domain's statistics
    domain_informed = False
    for domain in domains:
        if domain['primaryDomain'] == document['primaryDomain_s'] and domain['lang'] == 'fr':
            domain_informed = True
            domain['count_fulfill'] += 1
            domain['avg_similarity'] += similarity['avg_similarity']
            domain['count_valid100'] += similarity['count_valid100_tmp']
            domain['count_valid80'] += similarity['count_valid80_tmp']
            domain['count_valid60'] += similarity['count_valid60_tmp']
            domain['avg_keywords_unknown'] += similarity['avg_keywords_unknown']
            domain['avg_words_unknown'] += similarity['avg_words_unknown']

    if not domain_informed:
        domains.append({'primaryDomain': document['primaryDomain_s'], 'lang': 'fr', 'count_fulfill': 1, 'count_valid100': similarity['count_valid100_tmp'], 'count_valid80': similarity['count_valid80_tmp'], 'count_valid60': similarity['count_valid60_tmp'], 'avg_similarity': similarity['avg_similarity'], 'avg_words_unknown': similarity['avg_words_unknown'], 'avg_keywords_unknown': similarity['avg_keywords_unknown']})

    # Update type's statistics
    type_informed = False
    for type in types:
        if type['docType'] == document['docType_s'] and type['lang'] == 'fr':
            type_informed = True
            type['count_fulfill'] += 1
            type['avg_similarity'] += similarity['avg_similarity']
            type['count_valid100'] += similarity['count_valid100_tmp']
            type['count_valid80'] += similarity['count_valid80_tmp']
            type['count_valid60'] += similarity['count_valid60_tmp']
            type['avg_keywords_unknown'] += similarity['avg_keywords_unknown']
            type['avg_words_unknown'] += similarity['avg_words_unknown']

    if not type_informed:
        types.append({'docType': document['docType_s'], 'lang': 'fr', 'count_fulfill': 1, 'count_valid100': similarity['count_valid100_tmp'], 'count_valid80': similarity['count_valid80_tmp'], 'count_valid60': similarity['count_valid60_tmp'], 'avg_similarity': similarity['avg_similarity'], 'avg_words_unknown': similarity['avg_words_unknown'], 'avg_keywords_unknown': similarity['avg_keywords_unknown']})

    # Update yearly statistics
    yearly_informed = False
    for yearly in yearlies:
        if yearly['publicationDate'] == document['publicationDateY_i'] and yearly['lang'] == 'fr':
            yearly_informed = True
            yearly['count_fulfill'] += 1
            yearly['avg_similarity'] += similarity['avg_similarity']
            yearly['count_valid100'] += similarity['count_valid100_tmp']
            yearly['count_valid80'] += similarity['count_valid80_tmp']
            yearly['count_valid60'] += similarity['count_valid60_tmp']
            yearly['avg_keywords_unknown'] += similarity['avg_keywords_unknown']
            yearly['avg_words_unknown'] += similarity['avg_words_unknown']

    if not yearly_informed:
        yearlies.append({'publicationDate': document['publicationDateY_i'], 'lang': 'fr', 'count_fulfill': 1, 'count_valid100': similarity['count_valid100_tmp'], 'count_valid80': similarity['count_valid80_tmp'], 'count_valid60': similarity['count_valid60_tmp'], 'avg_similarity': similarity['avg_similarity'], 'avg_words_unknown': similarity['avg_words_unknown'], 'avg_keywords_unknown': similarity['avg_keywords_unknown']})
cursor.close()

for domain in domains:
    domain['avg_similarity'] = domain['avg_similarity'] / domain['count_fulfill']
    domain['avg_keywords_unknown'] = domain['avg_keywords_unknown'] / domain['count_fulfill']
    domain['avg_words_unknown'] = domain['avg_words_unknown'] / domain['count_fulfill']
    col = db['primaryDomain']
    col.insert(domain)

for type in types:
    type['avg_similarity'] = type['avg_similarity'] / type['count_fulfill']
    type['avg_keywords_unknown'] = type['avg_keywords_unknown'] / type['count_fulfill']
    type['avg_words_unknown'] = type['avg_words_unknown'] / type['count_fulfill']
    col = db['docType']
    col.insert(type)

for yearly in yearlies:
    yearly['avg_similarity'] = yearly['avg_similarity'] / yearly['count_fulfill']
    yearly['avg_keywords_unknown'] = yearly['avg_keywords_unknown'] / yearly['count_fulfill']
    yearly['avg_words_unknown'] = yearly['avg_words_unknown'] / yearly['count_fulfill']
    col = db['publicationDate']
    col.insert(yearly)

col = db['words'].insert(unknown_words)