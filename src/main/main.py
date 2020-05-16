'''
Created on May 16, 2020

@author: flickisc
'''

import pandas as pd
from elasticsearch import Elasticsearch

if __name__ == '__main__':
    # connect to local host
    es = Elasticsearch()
    
    # read in sample data
    df = pd.read_csv('../../res/book.csv')
    #df = df.fillna("NULL")
    # print out dataframe columns
    print(df.columns)
    print(df[['title', 'pages']].head(3))
    df = df.fillna(0)
    print("Deleting old book list...")
    es.indices.delete("books", ignore=[400, 404])
    print("Done with delete!")
    
    es.indices.create("books", {})
    print("starting book population...")
    id = 0
    for k in range(0, df.shape[0]):
        # parse book data
        book = df.iloc[k, :]
        author_arr = book.authors.split(", ")
        bookInfo = {"title": book.title, "authors": author_arr, 'pages': book.pages, 'isbn13': str(book['isbn13']), 'quantity': 2}
        # create new book entry
        es.create("books", id,
                   {"title": bookInfo['title'], "authors": bookInfo['authors'],
                    "pages": bookInfo['pages'], "isbn": bookInfo['isbn13']}, doc_type="_doc");
        id = id + 1
    
    print("done")
    
    