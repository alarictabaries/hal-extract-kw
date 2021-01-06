import pymongo
from libraries import nlprocessing


# Connect to MongoDB server
server = pymongo.MongoClient("mongodb://localhost:27017/")
db = server['hal']

bd_del = True
if bd_del:
    col = db['docType']
    col.drop()
    col = db['primaryDomain']
    col.drop()
    col = db['publicationDateY']
    col.drop()

col = db['articles_cleaned_fr']

# n = (2,575)² x (0,5)(1-0,5) / (0,01)² = 16576.5625
# n = (2,575)² x (0,5)(1-0,5) / (0,02)² = 4144.140625
sample_size = 4145

sample = col.aggregate([{"$sample": {"size": 4145}}])



domains = []
types = []
yearlies = []

for document in sample:
    abstract = document['fr_abstract_s'][0]
    keywords = document['fr_keyword_s']

    # Compute similarity between abstract and keywords
    document['similarity'] = nlprocessing.computeSimilarity(abstract, keywords, 'fr', True)

    count_80 = 0
    count_60 = 0

    if document['similarity']['raw_match'] > 0.8:
        count_80 = 1
    if document['similarity']['raw_match'] > 0.6:
        count_60 = 1

    # Update domain's statistics
    domain_informed = False
    for domain in domains:
        if domain['primaryDomain'] == document['primaryDomain_s']:
            domain_informed = True
            domain['count'] += 1
            domain['raw_match_80'] += count_80
            domain['raw_match_60'] += count_60
            domain['raw_match'] += document['similarity']['raw_match']
            domain['wordnet_similarity'] += document['similarity']['wordnet_similarity']
            domain['avg_keywords_unknown'] += document['similarity']['avg_keywords_unknown']
            domain['avg_words_unknown'] += document['similarity']['avg_words_unknown']

    if not domain_informed:
        domains.append({'primaryDomain': document['primaryDomain_s'],
                        'count': 1,
                        'raw_match': document['similarity']['raw_match'],
                        'raw_match_80': count_80,
                        'raw_match_60': count_60,
                        'wordnet_similarity': document['similarity']['wordnet_similarity'],
                        'avg_keywords_unknown': document['similarity']['avg_keywords_unknown'],
                        'avg_words_unknown': document['similarity']['avg_words_unknown']
                        })

    # Update type's statistics
    type_informed = False
    for type in types:
        if type['docType'] == document['docType_s']:
            type_informed = True
            type['count'] += 1
            type['raw_match_80'] += count_80
            type['raw_match_60'] += count_60
            type['raw_match'] += document['similarity']['raw_match']
            type['wordnet_similarity'] += document['similarity']['wordnet_similarity']
            type['avg_keywords_unknown'] += document['similarity']['avg_keywords_unknown']
            type['avg_words_unknown'] += document['similarity']['avg_words_unknown']

    if not type_informed:
        types.append({'docType': document['docType_s'],
                      'count': 1,
                      'raw_match': document['similarity']['raw_match'],
                      'raw_match_80': count_80,
                      'raw_match_60': count_60,
                      'wordnet_similarity': document['similarity']['wordnet_similarity'],
                      'avg_keywords_unknown': document['similarity']['avg_keywords_unknown'],
                      'avg_words_unknown': document['similarity']['avg_words_unknown']
                      })

    # Update type's statistics
    yearly_informed = False
    for yearly in yearlies:
        if yearly['publicationDateY'] == document['publicationDateY_i']:
            yearly_informed = True
            yearly['count'] += 1
            yearly['raw_match_80'] += count_80
            yearly['raw_match_60'] += count_60
            yearly['raw_match'] += document['similarity']['raw_match']
            yearly['wordnet_similarity'] += document['similarity']['wordnet_similarity']
            yearly['avg_keywords_unknown'] += document['similarity']['avg_keywords_unknown']
            yearly['avg_words_unknown'] += document['similarity']['avg_words_unknown']

    if not yearly_informed:
        yearlies.append({'publicationDateY': document['publicationDateY_i'],
                      'count': 1,
                      'raw_match': document['similarity']['raw_match'],
                      'raw_match_80': count_80,
                      'raw_match_60': count_60,
                      'wordnet_similarity': document['similarity']['wordnet_similarity'],
                      'avg_keywords_unknown': document['similarity']['avg_keywords_unknown'],
                      'avg_words_unknown': document['similarity']['avg_words_unknown']
                      })

for domain in domains:
    domain['lang'] = 'fr'
    domain['raw_match'] = domain['raw_match'] / domain['count']
    domain['wordnet_similarity'] = domain['wordnet_similarity'] / domain['count']
    domain['avg_keywords_unknown'] = domain['avg_keywords_unknown'] / domain['count']
    domain['avg_words_unknown'] = domain['avg_words_unknown'] / domain['count']
    domain['raw_match_80'] = domain['raw_match_80'] / domain['count']
    domain['raw_match_60'] = domain['raw_match_60'] / domain['count']

    col = db['primaryDomain']
    col.insert_one(domain)

for type in types:
    type['lang'] = 'fr'
    type['raw_match'] = type['raw_match'] / type['count']
    type['wordnet_similarity'] = type['wordnet_similarity'] / type['count']
    type['avg_keywords_unknown'] = type['avg_keywords_unknown'] / type['count']
    type['avg_words_unknown'] = type['avg_words_unknown'] / type['count']
    type['raw_match_80'] = type['raw_match_80'] / type['count']
    type['raw_match_60'] = type['raw_match_60'] / type['count']

    col = db['docType']
    col.insert_one(type)

for yearly in yearlies:
    yearly['lang'] = 'fr'
    yearly['raw_match'] = yearly['raw_match'] / yearly['count']
    yearly['wordnet_similarity'] = yearly['wordnet_similarity'] / yearly['count']
    yearly['avg_keywords_unknown'] = yearly['avg_keywords_unknown'] / yearly['count']
    yearly['avg_words_unknown'] = yearly['avg_words_unknown'] / yearly['count']
    yearly['raw_match_80'] = yearly['raw_match_80'] / yearly['count']
    yearly['raw_match_60'] = yearly['raw_match_60'] / yearly['count']

    col = db['publicationDateY']
    col.insert_one(yearly)