from pprint import pprint
# Gensim
import gensim
import gensim.corpora as corpora
import numpy as np
from gensim.models import LdaModel
from configuration import ConfigClass


class LDA_ranker:

    lda_model = None
    corpus = []
    term_list = []
    topic_dict = {}
    cosine_dict = {}
    id2word = None

    def __init__(self, term_list1):
        self.term_list = term_list1
        self.to_stem = ConfigClass().get__toStem()

    def create_corpus(self):
        # Create Dictionary
        self.id2word = corpora.Dictionary(self.term_list)
        #self.id2word = gensim.corpora.Dictionary(doc for doc in self.term_list)

        # Create Corpus
        texts = self.term_list
        # Term Document Frequency
        self.corpus = [self.id2word.doc2bow(text) for text in texts]

        #for line in texts:
        #    self.corpus.append(id2word.doc2bow(line, allow_update=True))
        self.build_LDA_model(self.id2word)

    def build_LDA_model(self, id2word):
        """self.lda_model = gensim.models.ldamodel.LdaModel(corpus=self.corpus,
                                                         id2word=self.id2word,
                                                         num_topics=10,
                                                         random_state=100,
                                                         update_every=1,
                                                         chunksize=100,
                                                         passes=10,
                                                         alpha='auto',
                                                         per_word_topics=True)"""
        #self.lda_model = gensim.models.LdaMulticore(self.corpus, num_topics=10, id2word=self.id2word,minimum_probability=0)
        if not self.to_stem:
            self.lda_model = LdaModel.load("model.txt")
        else:
            self.lda_model = LdaModel.load("model_stem.txt")

        for i in range(10):  # start the dict
            self.topic_dict[i] = []

        for tweet_idx in range(len(self.term_list)):  # every tweet prop>0.55 in topic list in dict
            topic_vector = self.lda_model[self.corpus[tweet_idx]]
            for topic_num, prob in topic_vector:
                if prob > 0.55:
                    self.topic_dict[topic_num].append(tweet_idx)

        #self.print_LDA_model()

    def print_LDA_model(self):
        # Print the Keyword in the 10 topics
        pprint(self.lda_model.print_topics())
        new_text = ['Coronavirus', 'less', 'dangerous', 'flu']
        temp = corpora.Dictionary([new_text])
        print("tweet_topic_vector", self.lda_model[self.corpus[0]])
        print("topic:", self.lda_model[self.corpus[0]][0][0])
        print("prob:", self.lda_model[self.corpus[0]][0][1])
        print("query_vector:", self.lda_model[temp.doc2bow(new_text)])
        print("query_tupel:", self.lda_model[temp.doc2bow(new_text)][0])
        print("query_topic:", self.lda_model[temp.doc2bow(new_text)][0][0])
        print("query_prob:", self.lda_model[temp.doc2bow(new_text)][0][1])

    # Function to sort the list of tuples by its second item
    def Sort_Tuple(self, query_vector):
        lst = len(query_vector)
        for i in range(0, lst):
            for j in range(0, lst - i - 1):
                if query_vector[j][1] > query_vector[j + 1][1]:
                    temp = query_vector[j]
                    query_vector[j] = query_vector[j + 1]
                    query_vector[j + 1] = temp
        return query_vector

    def prob(self, query_as_list):
        print("query_as_list", query_as_list)
        token = corpora.Dictionary([query_as_list])
        query_vector = self.lda_model[token.doc2bow(query_as_list)]
        sorted_vector = self.Sort_Tuple(query_vector)
        query_topic = sorted_vector[0][0]

        query_prob_vector = []
        for tuple in sorted_vector:
            query_prob_vector.append(tuple[1])
        cosine_dict = {}
        for index in self.topic_dict[query_topic]:
            index_vector = self.lda_model[self.corpus[index]]
            index_prob_vec = []
            for topic in index_vector:
                index_prob_vec.append(topic[1])
            cosine = self.cosine(query_prob_vector, index_prob_vec)
            cosine_dict[index] = cosine  # index-cosine dict
        return cosine_dict

    def cosine(self, query_prob_vector, index_prob_vec):
        v1 = np.array(query_prob_vector)
        v2 = np.array(index_prob_vec)
        cosine = np.dot(v1, v2) / (np.sqrt(np.sum(v1 * 2)) * np.sqrt(np.sum(v2 * 2)))
        return cosine