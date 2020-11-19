import pymongo
from libraries import nlprocessing
from pprint import pprint
from bson.objectid import ObjectId


# Connect to MongoDB server
server = pymongo.MongoClient("mongodb://localhost:27017/")
db = server['hal']
col = db['documents']

# Initialise some variables
count_fulfill = 0
# Set to false if in production
debug = True

unknown_words = [{'lang': 'fr', 'words': []}, {'lang': 'en', 'words': []}]

# Update with your stopwords
stopwords_homemade = [':', ',', ';', '.', '!', '?', ';', 'le', 'un', 'se', 'il', 'ce']

process_en = False
if process_en:
    # Select all documents with interesting fields in english
    cursor = col.find({"$and": [{"en_match": {"$exists": False}}, {"en_abstract_s": {"$exists": True}},
                                {"en_keyword_s": {"$exists": True}}]}, no_cursor_timeout=True).batch_size(100)
    for document in cursor:
        if debug:
            print('Current document :', end=' ')
            print(document)

        count_fulfill += 1
        abstract = document['en_abstract_s'][0]
        keywords = document['en_keyword_s']

        # Compute similarity between abstract and keywords
        similarity = nlprocessing.computeSimilarity(abstract, keywords, 'en', debug)
        # {'avg_similarity', 'count_valid100_tmp', 'count_valid80_tmp', 'count_valid60_tmp', 'keywords_unknown', 'words_unknown', 'avg_keywords_unknown', 'avg_words_unknown'}

        col.update_one({'_id': ObjectId(document['_id'])}, {"$set": {"en_match": similarity}})

    cursor.close()

process_fr = True
if process_fr:
    # Select all documents with interesting fields in french
    cursor = col.find({"$and": [{"fr_match": {"$exists": False}}, {"fr_abstract_s": {"$exists": True}},
                                {"fr_keyword_s": {"$exists": True}}]}, no_cursor_timeout=True).batch_size(100)
    for document in cursor:
        if debug:
            print('Current document :', end=' ')
            print(document)

        count_fulfill += 1
        abstract = document['fr_abstract_s'][0]
        keywords = document['fr_keyword_s']

        # Compute similarity between abstract and keywords
        similarity = nlprocessing.computeSimilarity(abstract, keywords, 'fr', debug)

        col.update_one({'_id': ObjectId(document['_id'])}, {"$set": {"fr_match": similarity}})
