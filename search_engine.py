import re
from datetime import datetime

from nltk.corpus import stopwords

from LDA_ranker import LDA_ranker
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
from stemmer import Stemmer


def run_engine():
    """

    :return:
    """
    number_of_documents = 0
    config = ConfigClass()
    corpus_path = config.get__corpusPath()
    r = ReadFile(corpus_path)
    p = Parse()
    indexer = Indexer(config)

    #reading per folder
    r.create_files_name_list()
    files_list = [] #every index contains all tweets per folder
    for file_name in r.dates_list:
        tweets_per_date = r.read_file(file_name)
        files_list.append(tweets_per_date)
    #print(len(files_list))

    """counter = 1
    # Iterate over every folder in the DATA
    for folder_list in files_list:
        print(counter)
        cr = datetime.now()
        print(cr)
        # Iterate over every tweet in the folder
        for idx, tweet in enumerate(folder_list):
            # parse the tweet
            parsed_document = p.parse_doc(tweet)
            number_of_documents += 1
            # index the tweet data
            indexer.add_new_doc(parsed_document)

        print("number of tweets", number_of_documents)
        cn = datetime.now()
        print(cn)
        counter += 1
    print('Finished parsing and indexing. Starting to export files')"""

    #read only one folder
    documents_list = r.read_file(file_name='')
    # Iterate over every document in the file
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = p.parse_doc(document)
        number_of_documents += 1
        # index the document data
        indexer.add_new_doc(parsed_document)
    print('Finished parsing and indexing. Starting to export files')

    utils.save_obj(indexer.inverted_idx, "inverted_idx")
    utils.save_obj(indexer.tfidfDict, "tfidfDict")


def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def search_and_rank_query(queries, inverted_index, k):
    config = ConfigClass()
    indexer = Indexer(config)
    to_stem = config.get__toStem()
    queries_list = []
    if type(queries) is list:  # if queries is a list
        for query in queries:
            queries_list.append(query)
    if type(queries) is str:  # if queries is a text file
        with open(queries, encoding='utf-8') as f:
            for line in f:
                queries_list.append(line)
    for query in queries_list:
        p = Parse()
        query_as_list = p.parse_sentence(query, 0)
        # adds upper case words to the query
        original_query_list = query.split(" ")
        stop_words = stopwords.words('english')
        original_query_list = [w for w in original_query_list if w not in stop_words]
        print(original_query_list)

        counter = 0
        while counter < len(original_query_list):  # find big terms
            len_term = 1
            word = original_query_list[counter]
            if word.isupper():  # NBA
                if word.find("\n") != -1:
                    word = word[:-1]
                    if word.find(".") != -1:
                        word = word[:-1]
                if not to_stem:
                    query_as_list.append(word)
                else:
                    stem_word = Stemmer().stem_term(word)
                    query_as_list.append(stem_word)
            elif len(word) > 1 and re.search('[a-zA-Z]', word) and word[0].isupper():  # upper first char
                term = word
                if original_query_list.index(word) + 1 < len(original_query_list):
                    index = original_query_list.index(word) + 1
                    while index < len(original_query_list):  # find all term
                        if len(original_query_list[index]) > 1 and re.search('[a-zA-Z]', original_query_list[index]) and \
                                original_query_list[index][0].isupper():
                            new_word2 = original_query_list[index][0] + original_query_list[index][1:].lower()  # Donald Trump
                            term += " " + new_word2
                            index += 1
                            len_term += 1
                        else:
                            break
                    if len_term > 1:
                        query_as_list.append(term)
            counter += len_term
        print(query_as_list)
        searcher = Searcher(inverted_index)
        relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
        print("relevant", len(relevant_docs))
        topic = LDA_ranker.query_topic(query_as_list)
        print("query_topic", topic)
        list_of_index = []
        for tweet in relevant_docs.keys():
            list_of_index.append(indexer.tweet_line_dict[tweet])  # list of line numbers for LDA
        same_topic_list = LDA_ranker.find_same_topic(list_of_index, topic)
        list_of_relevant_tweet = []
        for value in same_topic_list:
            for key in indexer.tweet_line_dict:
                if indexer.tweet_line_dict[key] == value:
                    list_of_relevant_tweet.append(key)  # list of tweet_id
        print("topic relevant", len(list_of_relevant_tweet))
        if len(relevant_docs) > 0:
            ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, list_of_relevant_tweet)

    return searcher.ranker.retrieve_top_k(ranked_docs, k)


#def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
def main():
    # config = ConfigClass()
    # config.set__toStem(stemming)
    # config.set__corpusPath(corpus_path)
    # config.set__savedFileMainFolder( output_path)
    # k = num_docs_to_retrieve
    # query = queries
    run_engine()
    #query = input("Please enter a query: ")
    #k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    #for doc_tuple in search_and_rank_query(query, inverted_index, k):
    num = 1
    for doc_tuple in search_and_rank_query("queries.txt", inverted_index, 5):
        print(num)
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
        num += 1