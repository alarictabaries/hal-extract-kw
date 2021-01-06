import requests


def findByFilter(domain, increment, getCount, dpIncrement):

    articles = []
    flags = 'halId_s,*_title_s,*_subTitle_s,*_keyword_s,*_abstract_s,primaryDomain_s,domainAllCode_s,publicationDateY_i,docType_s,files_s'

    filter = False

    if filter:
        req = requests.get('http://api.archives-ouvertes.fr/search?q=*:*&fq=domainAllCode_s:(' + domain + ')' + '&fl=' + flags + '&start=' + '&start=' + str(increment))
    else:
        req = requests.get('http://api.archives-ouvertes.fr/search?q=*:*' + '&fl=' + flags + '&start=' + '&start=' + str(increment))

    if req.status_code == 200:
        data = req.json()
        if "response" in data.keys():
            data = data['response']
            count = data['numFound']

            if getCount:
                return count

            for article in data['docs']:
                articles.append(article)

            if (count > 30) and (increment < (count)) and ((increment == 0) or (increment % dpIncrement != 0)):
                increment += 30
                tmp_articles = findByFilter(domain, increment, False, dpIncrement)
                for tmp_article in tmp_articles:
                    articles.append(tmp_article)

                return articles

            else:
                return articles

    else:
        print('Error % Wrong response from HAL API')
        return -1