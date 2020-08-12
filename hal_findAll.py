from libraries import hal
import json
import sys
import threading


def findAll():
    count = hal.findByFilter('shs.info', 0, True, 0)
    i = 0

    while i < count:
        articles = hal.findByFilter('shs.info', i, False, i + 12000)

        print('ETA : ' + str(len(articles)) + ' were OK')

        with open('data/abstracts_' + str(i) + '.json', 'w') as f:
            json.dump(articles, f, indent=4)

        i += 120000

    return


if __name__ == '__main__':
       sys.setrecursionlimit(100000)
       threading.stack_size(200000000)
       thread = threading.Thread(target=findAll)
       thread.start()