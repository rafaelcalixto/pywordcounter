### Python 3.6.8
from requests import get
from pymongo import MongoClient
from collections import Counter
from operator import itemgetter
from json import load
from re import findall

### This object has builded to do two tasks, get data from an API and persist
### in a MongoDB and get the data from the MongoDB and process to show the
### Top 10 words in the movie title with more than 5 letters.
class WordCounter:
    def __init__(self):
        # The URL of the API is preseted
        self.url = "http://openlibrary.org/search.json?q=lord"
        # Here the data to connect in the database is load from a JSON file
        self.connjs = load(open("database_conn.json"))
        # Here the connection is stabelished and the database is loaded to a variable
        self.conn = MongoClient(self.connjs["host"])
        self.db = self.conn[self.connjs["database"]]
    def GetData(self):
        ### This function get the data from the API and persist in the database
        ### if it wasn't done yet
        col = get(self.url).json()
        ## The data is persisted in two diferents collection: wordcounter and header
        ## to increase the performance in the queries
        if self.db["wordcounter"].count() == 0:
            # The collection is created automaticaly with the name "wordcounter"
            resp1 = self.db["wordcounter"].insert_many(col["docs"])
            print("The docs was loaded to the Database. Was inserted {}".format(
            len(resp1.inserted_ids)))
        else:
            print("The docs already was loaded to the Database")
        if self.db["header"].count() == 0:
            # The collection is created automaticaly with the name "header"
            del col["docs"]
            resp2 = self.db["header"].insert_one(col)
            print("The header was loaded to the Database with id {}".format(
            resp2.inserted_id))
        else:
            print("The header already was loaded to the Database")


    def GetMostPop(self):
        ### This function get the data from the database and process it to get
        ### the top 10 words in movie title with more than 5 letters
        words_list = self.db["wordcounter"].distinct("title_suggest")
        # Processing the data to take only words, droping special characters and
        # numbers
        titles = "".join( " " + e + " " for e in words_list if not e.isnumeric())
        titles = [ e for e in titles if e.isalpha() or e.isspace()]
        titles = "".join( e for e in titles)
        cont_titles = Counter([ t for t in titles.split(" ") if len(t) > 5])
        top10 = sorted(list(cont_titles.items()), key = itemgetter(1))[::-1]
        top10 = [e[0] for e in top10]

        ans = "The top 10 words in the movies title are:\n"
        ans += "1- {}\n2- {}\n3- {}\n4- {}\n5- {}\n".format(*top10[:5])
        ans += "6- {}\n7- {}\n8- {}\n9- {}\n10- {}".format(*top10[5:10])

        print(ans)

if __name__ == "__main__":
    wc = WordCounter()
    wc.GetData()
    wc.GetMostPop()
