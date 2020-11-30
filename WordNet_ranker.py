from nltk.corpus import wordnet as wn

class WordNet_ranker:

    query = []

    def __init__(self, query):
        self.query = query


    def extend_query (self):
        print(self.query)
        for term in self.query:
            synonyms = []
            hyponyms = []
            hypernyms = []
            #w1 = wn.synset(term+'.n.01')
            syn = wn.synsets(term)[0]
            print("syn", syn)  # Synset('child.n.01')
            print("name", syn.name())  # child.n.01
            print(syn.lemmas()[0].name())  # child
            for rule in syn.lemmas():
                synonyms.append(rule.name())
            res = []
            for word in synonyms:
                wordFromList1 = wn.synsets(term)[0]#print("wup_similarity", w1.wup_similarity(w2))
                wordFromList2 = wn.synsets(word)[0]# print(term)
                s = wordFromList1.wup_similarity(wordFromList2)# print(wn.synsets(term))
                res.append(s)
            new_term = ""
            max = 0
            index = 0
            for num in res:
                if float(num) > max:
                    if synonyms[index] != term:
                        new_term = synonyms[index]
                        max = float(num)
                index += 1
            if new_term != "":
                self.query.append(new_term)
            print("res", res)
            print("synonyms", synonyms)
        print(self.query)