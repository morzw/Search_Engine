import re
import threading
from datetime import datetime
from nltk.corpus import stopwords
from multiprocessing import Process, Lock

from WordNet_ranker import WordNet_ranker
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
from stemmer import Stemmer

config = ConfigClass()

#def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
def main():
    print("Start program")
    # TODO: write posting into output path
    """#config = ConfigClass()
    config.set__toStem(stemming)
    config.set__corpusPath(corpus_path)
    config.set__savedFileMainFolder(output_path)
    k = num_docs_to_retrieve
    query = queries

    if type(query) is list:  # if queries is a list
        if len(query) == 0:
            print("Tweet id: " + "{}" + " Score: " + "{}" + "\n")
    if type(query) is str:  # if queries is a text file
        with open(query, encoding='utf-8') as f:
            for line in f:
                if line == "":
                    print("Tweet id: " + "{}" + " Score: " + "{}" + "\n")"""

    lda = run_engine()
    #query = input("Please enter a query: ")
    #k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    """final_tweets = search_and_rank_query(query, inverted_index, k, lda)
    for query in final_tweets:
        num = 1
        for res in query:
            print("Tweet id: " + "{" + res + "}" + " Score: " + "{" + str(num) + "}")
            num += 1"""
    #for doc_tuple in search_and_rank_query(query, inverted_index, k):
    final_tweets = search_and_rank_query("queries.txt", inverted_index, 10, lda)
    for query in final_tweets:
        num = 1
        for res in query:
            print("Tweet id: " + "{" + res + "}" + " Score: " + "{" + str(num) + "}")
            num += 1

def run_engine():
    """

    :return:
    """
    number_of_documents = 0
    corpus_path = config.get__corpusPath()
    r = ReadFile(corpus_path)
    indexer = Indexer(config)
    p = Parse()

    #reading per folder
    r.create_files_name_list()
    files_list = []  # every index contains all tweets per folder
    for file_name in r.dates_list:
        tweets_per_date = r.read_file(file_name)
        files_list.append(tweets_per_date)
    print("files_list", len(files_list))

    num_of_tweets = 0
    for folder_list in files_list:
        num_of_tweets += len(folder_list)
    print("num_of_tweets", num_of_tweets)

    """#reading per folder
    r.create_files_name_list()
    threads = []
    for file_name in r.dates_list:
        t = threading.Thread(target=r.read_file(file_name))
        threads.append(t)
        t.start()
    print("files_list", r.files_list)"""

    """counter = 1
    procs = []
    # Iterate over every folder in the DATA
    for folder_list in files_list:
        proc = Process(target=test, args=(folder_list, counter, indexer, number_of_documents,))
        procs.append(proc)
        proc.start()
    # complete the processes
    for proc in procs:
        proc.join()
    print('Finished parsing and indexing. Starting to export files')"""


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
            indexer.add_new_doc(parsed_document, num_of_tweets)

        print("number of tweets", number_of_documents)
        cn = datetime.now()
        print(cn)
        counter += 1
    print('Finished parsing and indexing. Starting to export files')"""

    #read only one folder
    documents_list = r.read_file(file_name='')
    num_indexed = len(documents_list)

    # Iterate over every document in the file
    for idx, document in enumerate(documents_list):
        # parse the document
        parsed_document = p.parse_doc(document)
        number_of_documents += 1
        # index the document data
        indexer.add_new_doc(parsed_document, num_indexed)
    print('Finished parsing and indexing. Starting to export files')

    utils.save_obj(indexer.inverted_idx, "inverted_idx")
    utils.save_obj(indexer.tf_idf_dict, "tf_idf_dict")
    return indexer.get__lda__()


def test(folder_list, counter, indexer, number_of_documents):
    print(counter)
    cr = datetime.now()
    print(cr)
    p = Parse()
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

def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def search_and_rank_query(queries, inverted_index, k, lda):
    print("start:", datetime.now())

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
                if line != "\n":
                    queries_list.append(line)
    all_results = []
    query_num = 1
    tweet_id_num = 1
    for query in queries_list:
        p = Parse()
        # parse LDA query
        tokenized_query = p.parse_sentence(query, 0)
        original_query_list = query.split(" ")
        stop_words = stopwords.words('english')
        original_query_list = [w for w in original_query_list if w not in stop_words]
        # find long terms and upper case words
        counter = 0
        while counter < len(original_query_list):
            len_term = 1
            word = original_query_list[counter]
            if word.isupper():  # NBA
                if word.find("\n") != -1:
                    word = word[:-1]
                    if word.find(".") != -1:
                        word = word[:-1]
                if not to_stem:
                    tokenized_query.append(word)
                else:
                    stem_word = Stemmer().stem_term(word)
                    tokenized_query.append(stem_word)
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
                        tokenized_query.append(term)
            counter += len_term
        #print(tokenized_query)
        # WordNet query
        wn = WordNet_ranker(tokenized_query)
        WordNet_query = wn.extend_query()
        print("WordNet_query", WordNet_query)
        searcher = Searcher(inverted_index)
        print("inverted_index", len(inverted_index))
        # find relevant_docs
        relevant_docs = searcher.relevant_docs_from_posting(WordNet_query)
        print("relevant", len(relevant_docs))
        # find LDA relevant
        cosine_dict = lda.prob(tokenized_query)
        print("cosine dict", len(cosine_dict))

        dict_of_cosine_tweets = {}
        #list out keys and values separately
        key_list = list(indexer.tweet_line_dict.keys())
        val_list = list(indexer.tweet_line_dict.values())
        for index in cosine_dict.keys():  # find the tweet id
            dict_of_cosine_tweets[key_list[val_list.index(index)]] = cosine_dict[index]
        #print("finish_topic relevant", len(dict_of_cosine_tweets))

        final_dict = {}
        for tweet_id in dict_of_cosine_tweets.keys():
            if k > len(final_dict):
                if tweet_id in relevant_docs:
                    final_dict[tweet_id] = 0
                    final_dict[tweet_id] += (relevant_docs[tweet_id]+dict_of_cosine_tweets[tweet_id])

        sorted_cosine_tweets = {k: v for k, v in
                                sorted(final_dict.items(), key=lambda item: item[1], reverse=True)}
        final_tweets = list(sorted_cosine_tweets.keys())
        print("final before add K", len(final_tweets))
        if k > len(final_tweets):
            for key in relevant_docs.keys():
                if key not in final_dict:
                    if k > len(final_tweets):
                        final_tweets.append(key)
                    if k == len(final_tweets):
                        break
        print("final after K", len(final_tweets))
        #print("relevant", relevant_docs)

        #print("sorted_cosine_tweets", sorted_cosine_tweets)

        """for tweet in relevant_docs.keys():
            if tweet in list_of_cosine_tweets:
                if len(final_tweets) < k:
                    final_tweets.append(tweet)

        if len(final_tweets) < k:
            sorted_cosine_tweets = {k: v for k, v in
                                    sorted(list_of_cosine_tweets.items(), key=lambda item: item[1], reverse=True)}
            for key in sorted_cosine_tweets:
                if k > len(final_tweets) and key not in final_tweets:
                    final_tweets.append(key)
                else:
                    break"""

        # write the results into csv file
        tweet_id_num = 1
        s = ""
        with open('results.csv', 'a', encoding='utf-8') as fp:
            for p in final_tweets:
                s = ("Tweet id: " + "{"+p+"}" + " Score: " + "{"+str(tweet_id_num)+"}" + "\n")
                tweet_id_num += 1
                fp.write(s)
        query_num += 1
        all_results.append(final_tweets)
    print("end:", datetime.now())

    # return top K of final_tweets
    return all_results


