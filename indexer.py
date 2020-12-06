import collections
import copy
import os

from LDA_ranker import LDA_ranker
from configuration import ConfigClass
from stemmer import Stemmer
from multiprocessing import Lock, Manager


class Indexer:

    posting_file_num = 1
    file_counter = 1
    file_name_list = []
    finished_inverted = False
    LDA_list = []
    # LDA
    tweet_line_dict = {}
    line_number = 0
    lda = None
    #lock = Lock()
    cur_num_of_tweets = 0
    writen_terms = 0
    zipf = {}

    def __init__(self, config):
        self.inverted_idx = {}
        #manager = Manager()
        #self.temp_posting_dict = manager.dict()
        self.temp_posting_dict = {}
        self.copy_posting_dict = {}
        self.sorted_posting_dict = {}
        self.tf_idf_dict = {}
        self.sorted_term_dict = {}
        self.config = config
        self.path = self.config.get__savedFileMainFolder()+"\\"

    def add_new_doc(self, document, num_of_tweets):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param lock:
        :param capital_letter_dict:
        :param document: a document need to be indexed.
        :return: -
        """
        self.cur_num_of_tweets += 1
        document_dictionary = document.term_doc_dictionary

        # Update tf-idf dict
        tweet_id = document.tweet_id
        self.tf_idf_dict[tweet_id] = []       # max_tf         # distinct_words        # tweet_length
        self.tf_idf_dict[tweet_id].append((document.max_tf, document.distinct_words, document.doc_length))


        # Go over each term in the doc
        term_list_to_LDA = []
        if len(self.temp_posting_dict) < 500000 and document.doc_length != -1:
            for term in document_dictionary.keys():
                try:
                    # Update posting
                    if term not in self.temp_posting_dict.keys():
                        self.temp_posting_dict[term] = []
                        self.temp_posting_dict[term].append([document.tweet_id, document_dictionary[term][0], document_dictionary[term][1]])

                    else:
                        self.temp_posting_dict[term].append([document.tweet_id, document_dictionary[term][0], document_dictionary[term][1]])
                except:
                    print('problem with the following key {}'.format(term[0]))
                term_list_to_LDA.append(term)
            self.LDA_list.append(term_list_to_LDA)  # add to LDA list
            self.tweet_line_dict[document.tweet_id] = self.line_number  # tweet_id, line_num
            self.line_number += 1

        else:  # len(self.temp_posting_dict) == 500000
            #self.lock.acquire()
            if document.doc_length != -1:
                # copy temp_posting_dict
                self.copy_posting_dict = copy.deepcopy(self.temp_posting_dict)
                # empty temp_posting_dict
                self.temp_posting_dict.clear()
                # sort the dict
                self.sorted_posting_dict = collections.OrderedDict(sorted(self.copy_posting_dict.items()))
                # empty copy_posting_dict
                self.copy_posting_dict.clear()
                #print("*********************************************")
                # make a txt file out of the sorted_posting_dict
                with open(self.path+'posting' + str(self.file_counter) + '.txt', 'w', encoding='utf-8') as fp:
                    for p in self.sorted_posting_dict.items():
                        for str1 in p[1]:
                            self.writen_terms += 1
                            s = p[0] + ":" + str(str1[0]) + "-" + str(str1[1]) + "-" + str(str1[2])[1:-1]
                            fp.write(s+"\n")
                #print("*********************************************")
                # empty copy_posting_dict
                self.sorted_posting_dict.clear()
                self.file_name_list.append('posting' + str(self.file_counter) + '.txt')
                self.file_counter += 1
                # write the corpus to the disk
                with open('LDA.txt', 'a', encoding='utf-8') as fp:
                    for p in self.LDA_list:
                        s = ""
                        for term in p:
                            s += term+" "
                        fp.write(s + "\n")
                self.LDA_list.clear()
                #self.lock.release()

        if self.cur_num_of_tweets == num_of_tweets and len(self.temp_posting_dict) > 0:  # if last tweet
            #self.lock.acquire()
            # copy temp_posting_dict
            self.copy_posting_dict = copy.deepcopy(self.temp_posting_dict)
            # empty temp_posting_dict
            self.temp_posting_dict.clear()
            # sort the dict
            self.sorted_posting_dict = collections.OrderedDict(sorted(self.copy_posting_dict.items()))
            # empty copy_posting_dict
            self.copy_posting_dict.clear()
            #print("*********************************************")
            # make a txt file out of the sorted_posting_dict
            with open(self.path+'posting' + str(self.file_counter) + '.txt', 'w', encoding='utf-8') as fp:
                for p in self.sorted_posting_dict.items():
                    for str1 in p[1]:
                        self.writen_terms += 1
                        s = p[0] + ":" + str(str1[0]) + "-" + str(str1[1]) + "-" + str(str1[2])[1:-1]
                        fp.write(s+"\n")
            #print("*********************************************")
            # empty copy_posting_dict
            self.sorted_posting_dict.clear()
            self.file_name_list.append('posting' + str(self.file_counter) + '.txt')
            self.file_counter += 1
            # write the corpus to the disk
            with open('LDA.txt', 'a', encoding='utf-8') as fp:
                for p in self.LDA_list:
                    s = ""
                    for term in p:
                        s += term+" "
                    fp.write(s + "\n")
            self.LDA_list.clear()
            #self.lock.release()

        time_to_merge = False
        # create new file of term_dict
        if self.cur_num_of_tweets == num_of_tweets:
            # sort the dict
            self.sorted_term_dict = collections.OrderedDict(sorted(document.term_dict.items()))
            # make a txt file out of the term_dict
            with open(self.path+'posting' + str(self.file_counter) + '.txt', 'w', encoding='utf-8') as fp:
                for p in self.sorted_term_dict.items():
                    if len(p[1]) > 1:  # more then 2 tweet_id
                        for str1 in p[1]:
                            self.writen_terms += 1
                            s = p[0] + ":" + str(str1[0]) + "-" + str(str1[1]) + "-100"
                            fp.write(s + "\n")
            self.file_name_list.append('posting' + str(self.file_counter) + '.txt')
            # empty sorted_term_dict
            self.sorted_term_dict.clear()
            self.file_counter += 1
            time_to_merge = True

        # merge all files to one
        if time_to_merge:
            while len(self.file_name_list) > 1:
                #print(self.file_name_list)
                self.merge_sorted_files(self.file_name_list[0], self.file_name_list[1])
                # remove all files and names
                os.remove(self.path+self.file_name_list[1])
                os.remove(self.path+self.file_name_list[0])
                self.file_name_list.remove(self.file_name_list[1])
                self.file_name_list.remove(self.file_name_list[0])
            # finished making one big posting file
            self.create_inverted_index(self.file_name_list[0])

        # Change all capital letter terms in dict
        if self.finished_inverted:
            #config = ConfigClass()
            to_stem = self.config.get__toStem()
            for term in document.capital_letter_dict:
                if document.capital_letter_dict[term]:  # if the term is upper is all corpus
                    if not to_stem:
                        if term.lower() in self.inverted_idx:
                            self.inverted_idx[term] = self.inverted_idx[term.lower()]
                            del self.inverted_idx[term.lower()]
                    else:
                        stem_term = Stemmer().stem_term(term)
                        if term.lower() != stem_term:
                            if stem_term.lower() in self.inverted_idx:
                                self.inverted_idx[stem_term.upper()] = self.inverted_idx[stem_term.lower()]
                        else:
                            if stem_term.lower() in self.inverted_idx:
                                self.inverted_idx[stem_term.upper()] = self.inverted_idx[stem_term.lower()]
                                del self.inverted_idx[term.lower()]


        if self.finished_inverted:
            """with open('LDA.txt', 'w', encoding='utf-8') as fp:
                for p in self.LDA_list:
                    s = ""
                    for term in p:
                        s += term+" "
                    fp.write(s + "\n")"""
            # read the corpus from file
            with open('LDA.txt', buffering=2000000, encoding='utf-8') as f:
                for line in f:
                    sp_line = line.split(" ")
                    self.LDA_list.append(sp_line)
            os.remove('LDA.txt')

            # add long term into LDA list
            for term in document.term_dict:
                for ID in document.term_dict[term]:
                    if ID[1] > 1:
                        tweet_id = ID[0]
                        if tweet_id in self.tweet_line_dict:
                            index = self.tweet_line_dict[tweet_id]
                            self.LDA_list[index].append(term)
            # empty term_dict
            document.term_dict.clear()
            self.lda = LDA_ranker(self.LDA_list)  # start LDA ranker
            # empty LDA_list
            #self.LDA_list.clear()
            self.lda.create_corpus()
            #return lda

    def get__lda__(self):
        return self.lda

    def read_non_empty_line(self, file):
        while True:
            line = file.readline()
            if line == "":  # end of the file
                return ""
            if line.isspace() == False:
                return line.strip()

    def merge_sorted_files(self, file1, file2):
        #print('##################################')
        read_file1, read_file2 = True, True
        with open(self.path+'posting' + str(self.file_counter) + '.txt', 'w', encoding='utf-8') as output_file:
            with open(self.path+file1, 'r', encoding='utf-8') as input_file1:
                with open(self.path+file2, 'r', encoding='utf-8') as input_file2:
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

        self.file_name_list.append('posting' + str(self.file_counter) + '.txt')
        self.file_counter += 1

    def create_inverted_index(self, file_name):
        with open(self.path+file_name, buffering=2000000, encoding='utf-8') as f:
            num_of_lines = 1
            count = 1
            posting_string = []
            post_line = self.writen_terms / 3
            for line in f:
                posting_string.append(line)
                split_line = line.split(":")
                term = split_line[0]
                #tf = line.split("-")[-2]
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = []
                    self.inverted_idx[term].append([1, self.path+'posting' + str(self.posting_file_num) + '.txt'])  # num of tweets, pointer
                else:
                    self.inverted_idx[term][0][0] += 1
                # break the big posting file into smaller files
                if num_of_lines == int(post_line):
                    if count <= 2:
                        #print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                        with open(self.path+'posting' + str(self.posting_file_num) + '.txt', 'w', encoding='utf-8') as fp:
                            for p in posting_string:
                                fp.write(p)
                        #print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                        self.posting_file_num += 1
                        num_of_lines = 0
                        posting_string = []
                        count += 1
                num_of_lines += 1

            # adding the last terms to new posting file
            if len(posting_string) > 0:
                #print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                with open(self.path+'posting' + str(self.posting_file_num) + '.txt', 'w', encoding='utf-8') as fp:
                    for p in posting_string:
                        fp.write(p)
                #print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                self.posting_file_num += 1
        if self.file_counter > self.posting_file_num:
            os.remove(self.path+self.file_name_list[0])
        self.finished_inverted = True




