class Indexer:

    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.tfidfDict = {}
        self.config = config

    def add_new_doc(self, document, capital_letter_dict, term_dict):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param capital_letter_dict:
        :param document: a document need to be indexed.
        :return: -
        """
        document_dictionary = document.term_doc_dictionary

        # Update tf-idf dict
        tweet_id = document.tweet_id
        self.tfidfDict[tweet_id] = []
        self.tfidfDict[tweet_id].append((document.max_tf, document.distinct_words))

        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = 1
                    self.postingDict[term] = []
                else:
                    self.inverted_idx[term] += 1

                self.postingDict[term].append((document.tweet_id, document_dictionary[term]))

            except:
                print('problem with the following key {}'.format(term[0]))

        # Change all capital letter terms in dict
        if len(capital_letter_dict) != 0:
            for term in capital_letter_dict:
                if capital_letter_dict[term]:  # if the term is upper is all corpus
                    if term.lower() in self.inverted_idx:
                        self.inverted_idx[term] = self.inverted_idx[term.lower()]
                        del self.inverted_idx[term.lower()]

        # append all terms to dict
        if len(term_dict) != 0:
            for term in term_dict:
                if term not in self.inverted_idx:
                    self.inverted_idx[term] = len(term_dict[term])

        #print(self.inverted_idx)
        #print(len(self.inverted_idx))






