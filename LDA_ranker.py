from pprint import pprint
# Gensim
import gensim
import gensim.corpora as corpora

class LDA_ranker:

    lda_model = []
    corpus = []
    term_list = []

    def __init__(self, term_list1):
        self.term_list = term_list1

    # def set__term_list(term_list):
    #     term_list = term_list

    def create_corpus(self):
        # Create Dictionary
        id2word = corpora.Dictionary(self.term_list)
        # Create Corpus
        texts = self.term_list
        # Term Document Frequency
        self.corpus = [id2word.doc2bow(text) for text in texts]
        # View
        print(self.corpus[:20])

        # Human readable format of corpus (term-frequency)
        # [[(id2word[id], freq) for id, freq in cp] for cp in corpus[:1]]

        self.build_LDA_model(id2word)

    def build_LDA_model(self, id2word):
        self.lda_model = gensim.models.ldamodel.LdaModel(corpus=self.corpus,
                                                         id2word=id2word,
                                                         num_topics=10,
                                                         random_state=100,
                                                         update_every=1,
                                                         chunksize=100,
                                                         passes=10,
                                                         alpha='auto',
                                                         per_word_topics=True)
        self.print_LDA_model()

    def print_LDA_model(self):
        # Print the Keyword in the 10 topics
        pprint(self.lda_model.print_topics())
        new_text = ['Coronavirus', 'less', 'dangerous', 'flu']
        temp = corpora.Dictionary([new_text])
        print("number1:", self.lda_model[self.corpus[0]][0][0])
        print("number1:", self.lda_model[self.corpus[0]][0][0][0])
        print("number1:", self.lda_model[self.corpus[0]][0][0][1])
        print("number2:", self.lda_model[temp.doc2bow(new_text)][0][0][0])

    def query_topic(self, query_as_list):
        print(len(self.corpus))
        print(len(self.lda_model))
        temp = corpora.Dictionary([query_as_list])
        print(query_as_list)
        return self.lda_model[temp.doc2bow(query_as_list)][0][0][0]

    def find_same_topic(self, list_of_lines, topic):
        list_of_index = []
        for index in list_of_lines:
            if topic == self.lda_model[self.corpus[index]][0][0][0]:
                list_of_index.append(index)
        return list_of_index
