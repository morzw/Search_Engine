from datetime import datetime

from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils


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
    utils.save_obj(indexer.temp_posting_dict, "posting")


def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    run_engine()
    query = input("Please enter a query: ")
    k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    for doc_tuple in search_and_rank_query(query, inverted_index, k):
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
