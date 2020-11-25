from parser_module import Parse
from ranker import Ranker


class Searcher:

    def __init__(self, inverted_index):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index

    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        relevant_docs = {}
        for term in query:
            if term in self.inverted_index:
                posting_file_name = self.inverted_index[term][0][1]
                with open(posting_file_name, buffering=2000000, encoding='utf-8') as f:
                    for line in f:
                        term_list = line.split(":")
                        key = term_list[0]
                        value = term_list[1]
                        if " " not in key:
                            key = key.lower()
                        if term == key:
                            try:
                                tweet_id = value.split("-")[0]
                                if tweet_id not in relevant_docs.keys():
                                    relevant_docs[tweet_id] = 1
                                else:
                                    relevant_docs[tweet_id] += 1
                            except:
                                print('term {} not found in posting'.format(term))
        return relevant_docs
