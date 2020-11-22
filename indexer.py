import collections
import copy
import json


class Indexer:

    num_of_terms_in_posting_dict = 0
    file_counter = 1
    res_counter = 1

    def __init__(self, config):
        self.inverted_idx = {}
        self.temp_posting_dict = {}
        self.copy_posting_dict = {}
        self.sorted_posting_dict = {}
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

        # # Go over each term in the doc
        # for term in document_dictionary.keys():
        #     try:
        #         # Update inverted index and posting
        #         if term not in self.inverted_idx.keys():
        #             self.inverted_idx[term] = 1
        #             self.postingDict[term] = []
        #         else:
        #             self.inverted_idx[term] += 1
        #
        #         self.postingDict[term].append((document.tweet_id, document_dictionary[term]))
        #
        #     except:
        #         print('problem with the following key {}'.format(term[0]))

        # Go over each term in the doc
        if len(self.temp_posting_dict) < 10000:
            for term in document_dictionary.keys():
                    try:
                        # Update posting
                        if term not in self.temp_posting_dict.keys():
                            self.num_of_terms_in_posting_dict += 1
                            self.temp_posting_dict[term] = []
                            self.temp_posting_dict[term].append([document.tweet_id, document_dictionary[term][0], document_dictionary[term][1]])
                        else:
                            self.temp_posting_dict[term].append([document.tweet_id, document_dictionary[term][0], document_dictionary[term][1]])
                    except:
                        print('problem with the following key {}'.format(term[0]))
        else:  # num_of_terms_in_posting_dict == 100
            # copy temp_posting_dict
            self.copy_posting_dict = copy.deepcopy(self.temp_posting_dict)
            # empty temp_posting_dict
            self.temp_posting_dict.clear()
            # sort the dict
            self.sorted_posting_dict = collections.OrderedDict(sorted(self.copy_posting_dict.items()))
            # empty copy_posting_dict
            self.copy_posting_dict.clear()
            print("*********************************************")
            # make a txt file out of the sorted_posting_dict
            with open('posting' + str(self.file_counter) + '.txt', 'w', encoding='utf-8') as fp:
                for p in self.sorted_posting_dict.items():
                    for str1 in p[1]:
                        s = p[0] + ":" + str(str1[0]) + "-" + str(str1[1]) + "-" + str(str1[2])[1:-1]
                        fp.write(s+"\n")
            print("*********************************************")
            # empty copy_posting_dict
            self.sorted_posting_dict.clear()
            if self.file_counter > 0 and self.file_counter % 2 == 0:  # merge every two text files
                self.combine_sorted_files("posting"+str(self.file_counter-1)+".txt", "posting"+str(self.file_counter)+".txt")
            self.file_counter += 1




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
                    if len(term_dict[term]) > 1:
                        self.inverted_idx[term] = len(term_dict[term])

        """if len(term_dict) != 0:
            print(self.inverted_idx)
            print(len(self.inverted_idx))"""

    def read_non_empty_line(self, input):
        while True:
            line = input.readline()
            if line == "":  # end of the file
                return ""
            if line.isspace() == False:
                return line.strip()

    def combine_sorted_files(self, file1, file2):
        print('##################################')
        read_file1, read_file2 = True, True
        with open('res' + str(self.res_counter) + '.txt', 'w', encoding='utf-8') as output_file:
            with open(file1, 'r', encoding='utf-8') as input_file1:
                with open(file2, 'r', encoding='utf-8') as input_file2:
                    while True:
                        if read_file1:
                            line1 = self.read_non_empty_line(input_file1)  # read one line, skip empty line
                        if read_file2:
                            line2 = self.read_non_empty_line(input_file2)  # read one line, skip empty line

                        if line1 == "" or line2 == "":  # end of the file
                            break

                        read_file1, read_file2 = False, False
                        # find the keys of the line
                        idx1 = line1.index(":")
                        str1 = line1[:idx1]
                        idx2 = line2.index(":")
                        str2 = line2[:idx2]
                        # sort by keys
                        if str1 < str2:
                            smaller = line1
                            read_file1 = True
                        else:
                            smaller = line2
                            read_file2 = True

                        output_file.write(smaller)
                        output_file.write("\n")

                    while line1 != "":  # continue on file1 if necessary
                        output_file.write(line1)
                        output_file.write("\n")
                        line1 = self.read_non_empty_line(input_file1)
                    while line2 != "":  # continue on file2 if necessary
                        output_file.write(line2)
                        output_file.write("\n")
                        line2 = self.read_non_empty_line(input_file2)
        self.res_counter += 1




