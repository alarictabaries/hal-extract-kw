import pymongo
from nltk.corpus import wordnet
from nltk.corpus import stopwords
import spacy


# Loading french spacy model
nlp_en = spacy.load('en_core_web_sm')

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

# Update with your stopwords
stopwords_homemade = [':', ',', ';', '.', '!', '?', ';', 'le', 'un', 'se','il','ce']

# Select all documents with interesting fields
for document in col.find({"$and":[ {"en_abstract_s":{"$exists": True}}, {"en_keyword_s":{"$exists": True}}]}):
    # Confirm if select conditions worked well
    if ('en_abstract_s' in document) and ('en_keyword_s' in document):
        if debug:
            print('Current document :', end=' ')
            print(document)

        count_fulfill += 1
        abstract = document['en_abstract_s'][0]
        keywords = document['en_keyword_s']

        # Compute similarity between the abstract and the keywords

        ## Lemmatize keywords
        keywords_lemmas = []
        for keyword in keywords:
            keyword_tokenized = nlp_en(keyword)
            for keyword in keyword_tokenized:
                # Remove stopwords
                keywords_lemmas.append(keyword.lemma_)

        ## Remove stopwords
        for word in keywords_lemmas:
            if (word in stopwords.words('english')) or (word in stopwords_homemade):
                keywords_lemmas.remove(word)

        if debug:
            print('Current keywords lemmas :', end=' ')
            print(keywords_lemmas)

        ## Lemmatize summary
        abstract_tokenized = nlp_en(abstract)
        abstract_lemmas = []
        for word in abstract_tokenized:
            abstract_lemmas.append(word.lemma_)

        ## Remove stopwords
        for word in abstract_lemmas:
            if (word in stopwords.words('english')) or (word in stopwords_homemade):
                    abstract_lemmas.remove(word)

        if debug:
            print('Current abstract lemmas :', end=' ')
            print(abstract_lemmas)

        ## Compare keywords and abstracts
        threshold_100 = len(keywords_lemmas)
        threshold_80 = len(keywords_lemmas) * 0.8
        threshold_60 = len(keywords_lemmas) * 0.6

        current_score = 0
        avg_similarity = 0

        # Loop in keywords
        for keyword in keywords_lemmas:
            if debug:
                print('Current keyword syn :', end=' ')
                print(keyword)
            keyword_syn = wordnet.synsets(keyword)

            if debug:
                print('Current keyword syn :', end=' ')
                print(keyword_syn)

            # Loop (sub) in abstract
            for word in abstract_lemmas:
                if debug:
                    print('Current word :', end=' ')
                    print(word)

                # If words are similar, set similarity to one and skip
                if word == keyword:
                    max_similarity_tmp = 1
                # Else, compute the similarity using WOLF wordnet
                else:
                    word_syn = wordnet.synsets(word)

                    if debug:
                        print('Current word syn :', end=' ')
                        print(word_syn)

                    # Sometimes, it doesn't work (ex : "cassation" returns none synset)
                    if (len(keyword_syn) > 0) and (len(word_syn) > 0):

                        max_similarity_tmp = 0
                        current_similarity_tmp = keyword_syn[0].path_similarity(word_syn[0])

                        if debug:
                            print('Current similarity :', end=' ')
                            print(current_similarity_tmp)

                        if current_similarity_tmp is not None:
                            if max_similarity_tmp < current_similarity_tmp :
                                max_similarity_tmp = current_similarity_tmp

            # Summarize similarity scores for each keyword
            avg_similarity += max_similarity_tmp
            # Increment each time a keyword fully match another word in the abstract
            if max_similarity_tmp >= 1:
                current_score += 1

        # Check if 100% of keywords are matching
        count_valid100_tmp = 0
        if current_score >= threshold_100:
            count_valid100_tmp = 1
        # Check if 80% of keywords are matching
        count_valid80_tmp = 0
        if current_score >= threshold_80:
            count_valid80_tmp = 1
        # Check if 60% of keywords are matching
        count_valid60_tmp = 0
        if current_score >= threshold_60:
            count_valid60_tmp = 1

        # Update domain's statistics
        domain_informed = False
        for domain in domains:
            if domain['primaryDomain'] == document['primaryDomain_s']:
                domain_informed = True
                domain['count_fulfill'] += 1
                domain['avg_similarity'] += (avg_similarity / len(keywords_lemmas))
                domain['count_valid100'] += count_valid100_tmp
                domain['count_valid80'] += count_valid80_tmp
                domain['count_valid60'] += count_valid60_tmp

        if not domain_informed:
            domains.append({'primaryDomain': document['primaryDomain_s'], 'count_fulfill': 1, 'count_valid100': count_valid100_tmp, 'count_valid80': count_valid80_tmp, 'count_valid60': count_valid60_tmp, 'avg_similarity': (avg_similarity / len(keywords_lemmas))})

        # Update type's statistics
        type_informed = False
        for type in types:
            if type['docType'] == document['docType_s']:
                type_informed = True
                type['count_fulfill'] += 1
                type['avg_similarity'] += (avg_similarity / len(keywords_lemmas))
                type['count_valid100'] += count_valid100_tmp
                type['count_valid80'] += count_valid80_tmp
                type['count_valid60'] += count_valid60_tmp

        if not type_informed:
            types.append({'docType': document['docType_s'], 'count_fulfill': 1, 'count_valid100': count_valid100_tmp, 'count_valid80': count_valid80_tmp, 'count_valid60': count_valid60_tmp, 'avg_similarity': (avg_similarity / len(keywords_lemmas))})

        # Update yearly statistics
        yearly_informed = False
        for yearly in yearlies:
            if yearly['publicationDate'] == document['publicationDateY_i']:
                yearly_informed = True
                yearly['count_fulfill'] += 1
                yearly['avg_similarity'] += (avg_similarity / len(keywords_lemmas))
                yearly['count_valid100'] += count_valid100_tmp
                yearly['count_valid80'] += count_valid80_tmp
                yearly['count_valid60'] += count_valid60_tmp

        if not yearly_informed:
            yearlies.append({'productionDate': document['publicationDateY_i'], 'count_fulfill': 1, 'count_valid100': count_valid100_tmp, 'count_valid80': count_valid80_tmp, 'count_valid60': count_valid60_tmp, 'avg_similarity': (avg_similarity / len(keywords_lemmas))})

for domain in domains:
    domain['avg_similarity'] = domain['avg_similarity'] / domain['count_fulfill']
    col = db['primaryDomain']
    col.insert(domain)

for type in types:
    type['avg_similarity'] = type['avg_similarity'] / type['count_fulfill']
    col = db['docType']
    col.insert(type)

for yearly in yearlies:
    yearly['avg_similarity'] = yearly['avg_similarity'] / yearly['count_fulfill']
    col = db['productionDate']
    col.insert(yearly)
