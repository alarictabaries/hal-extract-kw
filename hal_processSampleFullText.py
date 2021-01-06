import pymongo
from libraries import nlprocessingFullText
from libraries import nlprocessing
from grobid.client import GrobidClient
import urllib.request
from bs4 import BeautifulSoup
import html


# Connect to MongoDB server
server = pymongo.MongoClient("mongodb://localhost:27017/")
db = server['hal']

bd_del = True
if bd_del:
    col = db['primaryDomain']
    col.drop()

col = db['articles_w_files_cleaned_fr']

# n = (2,575)² x (0,5)(1-0,5) / (0,01)² = 16576.5625
# n = (2,575)² x (0,5)(1-0,5) / (0,02)² = 4144.140625
sample_size = 100
progress = 0

sample = col.aggregate([{"$sample": {"size": sample_size}}])

domains = []

errors_count = 0

for document in sample:

    progress += 1
    print('Processing ' + str(progress) + "/" + str(sample_size))

    url = document['files_s'][0]
    print(url)

    try:
        response = urllib.request.urlopen(url)

        file = open("tmp/print.pdf", 'wb')
        file.write(response.read())
        file.close()

        client = GrobidClient("localhost", "8070")
        rsp, status = client.serve("processFulltextDocument", "tmp/print.pdf")

        soup = BeautifulSoup(rsp.content, 'lxml')
        fullText = soup.findAll(text=True)

        # Removing xml version tag
        fullText.pop(0)

        fullText_clean = []

        # Removing \n, etc.
        for piece in fullText:
            if html.unescape(piece).strip() != "" and piece != "GROBID - A machine learning software for extracting information from scholarly documents":
                fullText_clean.append(str(piece))

        keywords = document['fr_keyword_s']

        abstract = document['fr_abstract_s'][0]
        keywords = document['fr_keyword_s']

        if len(keywords) > 0 or len(fullText_clean) or abstract != "":

            # Compute similarity between abstract and keywords
            document['abstract_match'] = nlprocessing.computeSimilarity(abstract, keywords, 'fr', True)

            # Compute similarity between fullText and keywords
            document['fullText_match'] = nlprocessingFullText.computeSimilarity(fullText_clean, keywords, 'fr', True)

            # Update domain's statistics
            domain_already_registered = False
            for domain in domains:
                if domain['primaryDomain'] == document['primaryDomain_s']:
                    domain_already_registered = True
                    domain['count'] += 1
                    domain['fullText_match'] += document['fullText_match']
                    domain['abstract_match'] += document['abstract_match']

            if not domain_already_registered:
                domains.append({'primaryDomain': document['primaryDomain_s'],
                                'count': 1,
                                'fullText_match': document['fullText_match'],
                                'abstract_match': document['abstract_match']
                                })
        else:
            errors_count += 1

    # If can not retrieve the PDF file
    except:
        errors_count += 1
        print('Can not retrieve the PDF file')

for domain in domains:
    domain['lang'] = 'fr'
    domain['fullText_match'] = domain['fullText_match'] / domain['count']
    domain['abstract_match'] = domain['abstract_match'] / domain['count']

    col = db['primaryDomain']
    col.insert_one(domain)

print(errors_count)