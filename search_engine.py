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
        lda = indexer.add_new_doc(parsed_document)
    print('Finished parsing and indexing. Starting to export files')

    utils.save_obj(indexer.inverted_idx, "inverted_idx")
    utils.save_obj(indexer.tf_idf_dict, "tf_idf_dict")
    return lda

def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def search_and_rank_query(queries, inverted_index, k, lda):
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

    query_num = 1
    tweet_id_num = 1
    for query in queries_list:
        p = Parse()
        # parse the query
        query_as_list = p.parse_sentence(query, 0)
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
        # find relevant_docs
        relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
        print("relevant", len(relevant_docs))
        # find LDA relevant
        dict_of_relevant_tweet = lda.prob(query_as_list)
        print("cosine dict", len(dict_of_relevant_tweet))
        # list of tweet_id with high cosine
        list_of_cosine_tweets = []
        for key in dict_of_relevant_tweet.keys():
            for tweet, value in indexer.tweet_line_dict.items():
                if key == value:
                    list_of_cosine_tweets.append(tweet)
        print("finish_topic relevant", len(list_of_cosine_tweets))
        #if len(relevant_docs) > 0:
        #    ranked_docs = searcher.ranker.rank_relevant_doc(list_of_relevant_tweet)
        sorted_relevant_docs = {k: v for k, v in sorted(relevant_docs.items(), key=lambda item: item[1], reverse=True)}
        #print("relevant", sorted_relevant_docs)
        # find similar relevant_tweet - cosine_relevant
        final_tweets = []
        for tweet in sorted_relevant_docs.keys():
            if tweet in list_of_cosine_tweets:
                final_tweets.append(tweet)

        for tweet in final_tweets:
            if tweet in sorted_relevant_docs:
                del sorted_relevant_docs[tweet]

        if len(final_tweets) < k:
            for key in sorted_relevant_docs:
                if k > len(final_tweets):
                    final_tweets.append(key)
                else:
                    break

        # write the results into file
        tweet_id_num = 1
        s = ""
        with open('results.csv', 'a', encoding='utf-8') as fp:
            for p in final_tweets:
                if tweet_id_num <= k:
                    if tweet_id_num == 1:
                        s = ("Query number " + str(query_num) + ":" + "\n")
                    if tweet_id_num < k:
                        s += ("Tweet rank " + str(tweet_id_num) + ":" + "\n" + p + "\n")
                    if tweet_id_num == k:
                        s += ("Tweet rank " + str(tweet_id_num) + ":" + "\n" + p + "\n" + "\n")
                    tweet_id_num += 1
            fp.write(s)
        query_num += 1
    cn = datetime.now()
    print(cn)

    # return tok K of final_tweets
    return searcher.ranker.retrieve_top_k(final_tweets, k)


#def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
def main():
    print("Start program")
    # config = ConfigClass()
    # config.set__toStem(stemming)
    # config.set__corpusPath(corpus_path)
    # config.set__savedFileMainFolder( output_path)
    # k = num_docs_to_retrieve
    # query = queries
    lda = run_engine()
    #query = input("Please enter a query: ")
    #k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    #for doc_tuple in search_and_rank_query(query, inverted_index, k):
    num = 1
    for doc_tuple in search_and_rank_query("queries.txt", inverted_index, 5, lda):
        print(num)
        print(doc_tuple)
        #print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
        num += 1