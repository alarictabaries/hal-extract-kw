from nltk.corpus import wordnet
from nltk.corpus import stopwords
import spacy
import sys


# Compute similarity between the abstract and the keywords
def computeSimilarity(abstract, keywords, lang, debug):

    # Update with your stopwords
    stopwords_homemade = ['/', '-PRON-', '-', '=', "n'", '«', '»', "l'", "c'", ':', ',', ';', '.', '!', '?', ';', 'le', 'un', 'se', 'il', 'ce', "d'", 'de', '(', ')', '"', "'"]

    if lang == 'fr':
        nlp = spacy.load('fr_core_news_sm')
        stopwords_language = 'french'
    elif lang == 'en':
        nlp = spacy.load('en_core_web_sm')
        stopwords_language = 'english'
    else:
        print('Error % Language not supported')
        sys.exit(1)

    ## Lemmatize keywords
    keywords_lemmas = []
    for keyword in keywords:
        keyword_tokenized = nlp(keyword)
        for keyword in keyword_tokenized:
            if keyword.lemma_ not in stopwords.words(stopwords_language) and keyword.lemma_ not in stopwords_homemade:
                keywords_lemmas.append(keyword.lemma_)

    if debug:
        print('Current keywords lemmas :', end=' ')
        print(keywords_lemmas)

    ## Lemmatize summary
    abstract_tokenized = nlp(abstract)
    abstract_lemmas = []
    for word in abstract_tokenized:
        if word.lemma_ not in stopwords.words(stopwords_language) and word.lemma_ not in stopwords_homemade:
            abstract_lemmas.append(word.lemma_)

    if debug:
        print('Current abstract lemmas :', end=' ')
        print(abstract_lemmas)

    current_score = 0
    avg_similarity = 0

    keywords_unknown_count = 0
    keywords_unknown_list = []
    words_unknown_count = 0
    words_unknown_list = []

    abstract_syns = []
    for word in abstract_lemmas:
        word_syn = wordnet.synsets(word)

        if len(word_syn) == 0:
            words_unknown_count += 1
            words_unknown_list.append(word)

        if len(word_syn) > 0:
            abstract_syns.append(word_syn[0])

    # Loop in keywords
    for keyword in keywords_lemmas:
        if debug:
            print('Current keyword syn :', end=' ')
            print(keyword)
        keyword_syn = wordnet.synsets(keyword)

        if len(keyword_syn) == 0:
            keywords_unknown_count += 1
            keywords_unknown_list.append(keyword)

        max_similarity_tmp = 0

        # Loop (sub) in abstract
        for word in abstract_lemmas:
            if debug:
                print('Current word :', end=' ')
                print(word)

            # If words are similar, set similarity to one and skip
            if word == keyword:
                max_similarity_tmp = 1

        if max_similarity_tmp >= 1 and len(keyword_syn) > 0:
            for word_syn in abstract_syns:
                max_similarity_tmp = 0
                current_similarity_tmp = keyword_syn[0].path_similarity(word_syn)

                if debug:
                    print('Current similarity :', end=' ')
                    print(current_similarity_tmp)

                if current_similarity_tmp is not None:
                    if max_similarity_tmp < current_similarity_tmp:
                        max_similarity_tmp = current_similarity_tmp

        # Summarize similarity scores for each keyword
        if max_similarity_tmp >= 0:
            avg_similarity += max_similarity_tmp
        # Increment each time a keyword fully match another word in the abstract
        if max_similarity_tmp >= 1:
            current_score += 1

    if len(keywords_lemmas) > 0:
        match = current_score / len(keywords_lemmas)
        avg_similarity = avg_similarity / len(keywords_lemmas)
        avg_keywords_unknown = keywords_unknown_count / len(keywords_lemmas)
    else:
        avg_similarity = 0
        match = 0
        avg_keywords_unknown = 0

    if len(abstract_lemmas) > 0:
        avg_words_unknown = words_unknown_count / len(abstract_lemmas)
    else:
        avg_words_unknown = 0

    if debug:
        print('Document similarity :', end=' ')
        print(avg_similarity)

    return {'raw_match': match, 'wordnet_similarity': avg_similarity, 'keywords_unknown': keywords_unknown_list, 'words_unknown': words_unknown_list, 'avg_keywords_unknown': avg_keywords_unknown, 'avg_words_unknown': avg_words_unknown}