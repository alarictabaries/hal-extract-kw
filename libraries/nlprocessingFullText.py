from nltk.corpus import wordnet
from nltk.corpus import stopwords
import spacy
import sys


# Compute similarity between the fullText and the keywords
def computeSimilarity(fullText, keywords, lang, debug):

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
                keywords_lemmas.append(keyword.lemma_.lower())

    if debug:
        print('Current keywords lemmas :', end=' ')
        print(keywords_lemmas)

    ## Lemmatize keywords
    fullText_lemmas = []
    for piece in fullText:
        piece_tokenized = nlp(piece)
        for word in piece_tokenized:
            if word.lemma_ not in stopwords.words(stopwords_language) and word.lemma_ not in stopwords_homemade:
                fullText_lemmas.append(word.lemma_.lower())

    if debug:
        print('Current fullText lemmas :', end=' ')
        print(fullText_lemmas)

    current_score = 0

    # Loop in keywords
    for keyword in keywords_lemmas:
        max_similarity_tmp = 0

        # Loop (sub) in fullText
        for word in fullText_lemmas:

            # If words are similar, set similarity to 1 and skip
            if word == keyword:
                max_similarity_tmp = 1

        # Increment each time a keyword fully match another word in the abstract
        if max_similarity_tmp >= 1:
            current_score += 1

    if len(keywords_lemmas) > 0:
        match = current_score / len(keywords_lemmas)
    else:
        match = 0

    if debug:
        print('Full text match :', end=' ')
        print(current_score, end=' / ')
        print(len(keywords_lemmas), end=' ')
        print("(" + str(match) + ")")

    return match