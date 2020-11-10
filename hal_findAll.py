from libraries import hal
import threading
import pymongo
from datetime import datetime

def findAll():
    count = hal.findByFilter('shs.info', 0, True, 0)
    # Set $i value to the amount of documents you've already collected
    # i = 0
    i = 2478180

    # Collect documents and save them to the DB every 12030 documents
    while i < count:
        articles = hal.findByFilter('shs.info', i, False, i + 12000)
        i += 12000

        # Save documents to mongoDB
        server = pymongo.MongoClient("mongodb://localhost:27017/")
        db = server['hal']
        col = db['documents_test']
        col.insert_many(articles)
        print(datetime.now().strftime("%H:%M:%S") )
        print('Processing...')

    return


if __name__ == '__main__':
    # sys.setrecursionlimit(100000)
    # threading.stack_size(64*1024)
    thread = threading.Thread(target=findAll)
    thread.start()
