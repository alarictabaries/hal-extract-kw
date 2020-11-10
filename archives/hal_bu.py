import requests

def findByFilter(domain, increment, getCount, dpIncrement):

    articles = []
    flags = 'keyword_s,domainAllCode_s,fr_abstract_s,domain_s'

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

            print('ETA : ' + str(increment) + '/' + str(count))

            for article in data['docs']:
                if ('keyword_s' in article) and ('fr_abstract_s' in article):
                    article['keyword_s'] = [x.lower() for x in article['keyword_s']]
                    article['fr_abstract_s'][0] = article['fr_abstract_s'][0].lower()

                    kw_in_abstract = [a for a in article['keyword_s'] if a in article['fr_abstract_s'][0]]
                    kw_in_abstract_pct = (len(kw_in_abstract) / len(article['keyword_s'])) * 100

                    article['keyword_s'] = kw_in_abstract

                    if kw_in_abstract_pct > 66:
                        articles.append(article)

            if (count > 30) and (increment < (count)) and ( (increment == 0) or (increment % dpIncrement != 0) ):
                increment += 30
                tmp_articles = findByFilter(domain, increment, False, dpIncrement)
                for tmp_article in tmp_articles:
                    if ('keyword_s' in tmp_article) and ('fr_abstract_s' in tmp_article):
                        tmp_article['keyword_s'] = [x.lower() for x in tmp_article['keyword_s']]
                        tmp_article['fr_abstract_s'][0] = tmp_article['fr_abstract_s'][0].lower()

                        kw_in_abstract = [a for a in tmp_article['keyword_s'] if a in tmp_article['fr_abstract_s'][0]]
                        kw_in_abstract_pct = (len(kw_in_abstract) / len(tmp_article['keyword_s'])) * 100

                        tmp_article['keyword_s'] = kw_in_abstract

                        if kw_in_abstract_pct > 66:
                            articles.append(tmp_article)
                return articles
            else:
                return articles
        else:
            print('Error : wrong response from the HAL server')
            return -1
    else:
        print('Error : can not contact HAL API server')
        return articles