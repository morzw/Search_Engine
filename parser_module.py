import re
from math import floor

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
from configuration import ConfigClass
from stemmer import Stemmer


class Parse:

    term_dict = {}
    capital_letter_dict = {}
    finished = False

    def __init__(self):
        self.stop_words = stopwords.words('english')
        self.to_stem = ConfigClass().get__toStem()

    def parse_term(self, tokenized_text, tweet_id):
        # Term Dict + Capital Letter Dict:
        counter = 0
        while counter < len(tokenized_text):
            len_term = 1
            word = tokenized_text[counter]
            if len(word) > 1 and word.islower() and counter-1 >= 0 and tokenized_text[counter-1] != '#':  # if found word lower in the corpus
                word_upper = word.upper()
                if word_upper in self.capital_letter_dict:
                    self.capital_letter_dict[word_upper] = False  # to remove afterwords
            if len(word) > 1 and re.search('[a-zA-Z]', word) and word[0].isupper() and counter-1 >= 0 and tokenized_text[counter-1] != '#':  # upper first char and not #
                term = word
                original_term = word
                if tokenized_text.index(word) + 1 < len(tokenized_text):
                    index = tokenized_text.index(word) + 1
                    """if len(tokenized_text[index]) > 1 and re.search('[a-zA-Z]', tokenized_text[index]) and tokenized_text[
                        index][0].isupper():  # next word is also upper first char
                        new_word = term[0] + term[1:].lower()
                        if new_word in self.term_dict:  # enter first word of term
                            self.term_dict[new_word].append(tweet_id)
                        else:
                            self.term_dict[new_word] = [tweet_id]"""
                    while index < len(tokenized_text):  # find all term
                        if len(tokenized_text[index]) > 1 and re.search('[a-zA-Z]', tokenized_text[index]) and tokenized_text[index][0].isupper():
                            original_word = tokenized_text[index]
                            new_word2 = tokenized_text[index][0] + tokenized_text[index][1:].lower()
                            """if new_word2 in self.term_dict:  # enter each word in term
                                self.term_dict[new_word2].append(tweet_id)
                            else:
                                self.term_dict[new_word2] = [tweet_id]"""
                            new_word2 = ''.join([i if ord(i) < 128 else '' for i in new_word2])# delete emoji
                            term += " " + new_word2
                            original_term += " " + original_word
                            index += 1
                            len_term += 1
                        else:
                            break

                if len_term == 1:  # appends to capital_letter_dict - key + num of tweets
                    term = ''.join([i if ord(i) < 128 else '' for i in term])  # delete emoji
                    self.capital_letter_dict[term.upper()] = True
                    """if term.upper() in self.capital_letter_dict:
                        self.capital_letter_dict[term.upper()][0] += 1
                    else:
                        self.capital_letter_dict[term.upper()] = True"""
                elif len_term > 1:  # appends to term_dict - key + tweet id
                    final_term = original_term.split(" ")
                    for word in final_term:
                        tokenized_text.remove(word)
                    found = False
                    if term in self.term_dict:
                        for tuple in self.term_dict[term]:
                            if tuple[0] == tweet_id:
                                found = True
                                tuple[1] += 1
                        if not found:
                            self.term_dict[term].append([tweet_id, 1])  # tweet_id , #shows in tweet
                    else:
                        self.term_dict[term] = [[tweet_id, 1]]
            counter += len_term
            """if tweet_id not in self.term_dict[term]:
                    self.term_dict[term].append([tweet_id, 1])
                else:
                    for tuple in self.term_dict[term]:
                        if tuple[0] == tweet_id:
                            self.term_dict[term][1] += 1"""
        #print(self.capital_letter_dict)
        #print(self.term_dict)

    def parse_sentence(self, text, tweet_id):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        # print(text)
        text_tokens = word_tokenize(text)
        if text_tokens[0] == 'RT':
            return []

        # find TAGS
        if "@" in text_tokens:
            index_list1 = [n for n, x in enumerate(text_tokens) if x == '@']
            counter = 0
            for index in index_list1:
                if index + 1 < len(text_tokens):
                    if text_tokens[index + 1] != '@':
                        new_term = text_tokens[index] + text_tokens[index + 1]
                        text_tokens.append(new_term)
                        counter += 1
            for sign in range(counter):  # deletes all '@' and the word after it from list
                rmv_index = text_tokens.index('@')
                if rmv_index + 1 < len(text_tokens):
                    if text_tokens[rmv_index + 1] != '@':
                        del text_tokens[rmv_index + 1]
                    else:
                        del text_tokens[rmv_index + 1]
                        del text_tokens[rmv_index + 1]
                text_tokens.remove('@')
##############################################################################################
        # find PERCENTAGES
        if "%" or "percent" or "Percent" or "percentage" or "Percentage" in text_tokens:
            index_list2 = [n for n, x in enumerate(text_tokens) if x == '%' or x == 'percent' or x == "percentage" or x == 'Percent' or x == "Percentage"]
            counter2 = 0
            for index in index_list2:
                if index - 1 >= 0:
                    if not re.search('[a-zA-Z]', text_tokens[index - 1]):
                        new_term = text_tokens[index - 1] + '%'
                        text_tokens.append(new_term)
                    if text_tokens[index] == '%':
                        counter2 += 1
            while counter2 > 0:  # deletes all '%' and the word after it from list
                rmv_index = text_tokens.index('%')
                if rmv_index + 1 < len(text_tokens) and text_tokens[rmv_index + 1] == '%': #if %%
                    del text_tokens[rmv_index + 1]
                    counter2 -= 1
                if rmv_index - 1 >= 0 and not re.search('[a-zA-Z]', text_tokens[rmv_index - 1]): #is number
                    del text_tokens[rmv_index]
                    del text_tokens[rmv_index - 1]
                counter2 -= 1
##############################################################################################
        # finding terms, entities and capital letter
        self.parse_term(text_tokens, tweet_id)
##############################################################################################
        # find NUMBERS
        numbers = []
        for item in text_tokens:  #([0-9]+[,.]+[0-9]+)  item.isnumeric() or item.isdigit() or item.isdecimal() or
            if re.findall("^\d+$|^[0-9]{1,3}([,.\/][0-9]{1,3}){0,6}$", item) and not re.search('[a-zA-Z]', item): #^\d+$|^[0-9]{1,3}([,.][0-9]{1,3})?$
                if item.find('-') == -1 and item.find('€') == -1 and item.find('£') == -1 and item.find(
                        '%') == -1 and item.find('¢') == -1 and item.find('~') == -1 and item.find('+') == -1 and item.find('/') <= 1 and item.find("'") == -1:
                    if item.find(',') == -1:
                        numbers.append(item)
                    elif item.find(',') != -1 and re.findall("^([0-9]{1,3})(,[0-9]{3})*$", item):
                        numbers.append(item)
        # if len(numbers) >0:
        #     print(numbers)
        fractions_list = []
        for num in numbers:
            occur = num.count('.')
            if occur < 2:  # not a date
                rmv_index = text_tokens.index(num)
                to_append = True
                no_text = True
                found_fractions = False
                if text_tokens[rmv_index].find("/") != -1 and rmv_index-1 > 0 and text_tokens[rmv_index-1].isnumeric():  # if found_fractions
                    all_fractions = text_tokens[rmv_index-1] + " " + text_tokens[rmv_index]
                    fractions_list.append(all_fractions)
                    found_fractions = True
                    to_append = True
                if rmv_index + 1 < len(text_tokens):  # yes text
                    if text_tokens[rmv_index + 1] == "million" or text_tokens[rmv_index + 1] == "Million" or \
                            text_tokens[rmv_index + 1] == "M" or text_tokens[rmv_index + 1] == "m" or text_tokens[rmv_index + 1] == "MILLION":
                        if len(num) < 6:
                            fixed_num = re.sub("[^\d\.]", "", num)  # remove comma
                            new_num = self.parse_numbers(str(float(fixed_num) * 1000000))
                        else:
                            new_num = self.parse_numbers(num)
                        no_text = False
                        text_tokens[rmv_index + 1] = " "  # remove from list
                        text_tokens[rmv_index] = " "
                    if text_tokens[rmv_index + 1] == "billion" or text_tokens[rmv_index + 1] == "Billion" or \
                            text_tokens[rmv_index + 1] == "B" or text_tokens[rmv_index + 1] == "b" or text_tokens[rmv_index + 1] == "BILLION":
                        if len(num) < 9:
                            fixed_num = re.sub("[^\d\.]", "", num)  # remove comma
                            new_num = self.parse_numbers(str(float(fixed_num) * 1000000000))
                        else:
                            new_num = self.parse_numbers(num)
                        no_text = False
                        text_tokens[rmv_index + 1] = " "  # remove from list
                        text_tokens[rmv_index] = " "
                    if text_tokens[rmv_index + 1] == "thousand" or text_tokens[rmv_index + 1] == "Thousand" or \
                            text_tokens[rmv_index + 1] == "K" or text_tokens[rmv_index + 1] == "k" or text_tokens[rmv_index + 1] == "THOUSAND":
                        if len(num) < 4:
                            fixed_num = re.sub("[^\d\.]", "", num)  # remove comma
                            new_num = self.parse_numbers(str(float(fixed_num) * 1000))
                        else:
                            new_num = self.parse_numbers(num)
                        no_text = False
                        text_tokens[rmv_index + 1] = " "  # remove from list
                        text_tokens[rmv_index] = " "
                    if not no_text:
                        text_tokens[rmv_index + 1]  # TODO:?????????????????
                if rmv_index - 1 >= 0 and text_tokens[rmv_index - 1] == '$':  # yes $
                    if no_text:
                        if len(num) > 3:
                            text_tokens.append("$" + self.parse_numbers(num))
                        else:
                            text_tokens.append("$" + num)
                        text_tokens[rmv_index] = " "  # remove $ from list
                        text_tokens[rmv_index - 1] = " "
                    else:
                        text_tokens.append("$" + new_num)
                        text_tokens[rmv_index-1] = " "  # remove $ from list
                    to_append = False
                if to_append:  # no $
                    if no_text:
                        if len(num) > 3:
                            text_tokens.append(self.parse_numbers(num))
                            text_tokens[rmv_index] = " "  # remove num from list
                    else:
                        text_tokens.append(new_num)
                if found_fractions:  # delete fractions
                    del text_tokens[rmv_index]
                    del text_tokens[rmv_index - 1]
        """punctuations = '''!(-+—[]{};:'",)<>,./?^&*_’~|=→"”“'''  # removes relevant punctuations and http and //short url
        index_count = 0
        for word in text_tokens:
            to_delete = False
            if len(word) > 1 and word.find('-') != -1:  # contains '-'
                text_tokens.extend(word.split('-'))
                text_tokens.remove(word)
                to_delete = True
            if len(word) > 1 and word.find('…') != -1:  # contains '…'
                if to_delete == False:
                    text_tokens.extend(word.split('…'))
                    text_tokens.remove(word)
                to_delete = True
            if len(word) > 1 and word.find('_') != -1:  # contains '_'
                if to_delete == False:
                    text_tokens.extend(word.split('_'))
                    text_tokens.remove(word)
                to_delete = True
            if len(word) > 1 and word.find('+') != -1:  # contains '+'
                if to_delete == False:
                    text_tokens.extend(word.split('+'))
                    text_tokens.remove(word)
                to_delete = True
            if len(word) > 1 and word.find('/') != -1 and not (word[0] == '/' and word[1] == '/'):  # contains '/'
                if to_delete == False:
                    text_tokens.extend(word.split('/'))
                    text_tokens.remove(word)
                to_delete = True
            if to_delete == False:
                if word in punctuations:
                    i = text_tokens.index(word)
                    text_tokens[i] = " "
                elif word == "http" or word == "https" or word == "http..." or word == "https..." or word == "RT" or word == "rt":
                    i2 = text_tokens.index(word)
                    text_tokens[i2] = " "
                elif len(word) > 1 and word[0] == '/' and word[1] == '/':
                    i3 = text_tokens.index(word)
                    text_tokens[i3] = " "
                else:
                    text_tokens[index_count] = ''.join([i if ord(i) < 128 else '' for i in word])
            index_count += 1
        text_tokens[:] = [x for x in text_tokens if
                          x != " " and x != ".." and x != "..." and x != "...." and x != "....." and x != "......" and
                          x != "``" and x != "''" and x != "'s" and x != "'m" and x != "n't" and x != "." and x != ""
                          and x != "'re" and x != "__" and x != "_" and x != "___" and x != "," and x != "!"]"""
##############################################################################################
        # find punctuations
        """new_words = []
        for word in text_tokens:
            word = re.sub('t.co.*|\'m|\'s|n\'t|\'re|\(|\)|\!|\-|\+|\[|\]|\{|\}|\;|\:|\'|\,|\<|\>|\?|\"|\^|\&|\*|\_|\~|\`|\||\=|\→|\/|\”|\“|\’|\—|\.|\``|\\\\|^http.*|^https.*|^RT$|^rt$',
                '', word, flags=re.IGNORECASE)
            word = ''.join([i if ord(i) < 128 else '' for i in word])
            if word == '' or word == ' ' or len(word) == 1:
                continue
            new_words.append(word)
        text_tokens = new_words"""
        new_words = []
        regex_pattern_for_num = '.*\d\.\d.*'
        regex_pattern_for_punctuation = 't.co.*|\'m|\'s|n\'t|\'re|\(|\)|\!|\-|\+|\[|\]|\{|\}|\;|\:|\'|\,|\<|\>|\?|\"|\^|\&|\*|\_|\~|\`|\||\=|\→|\/|\”|\“|\’|\—|\.|\``|\\\\|http.*|https.*|^RT$|^rt$'

        for word in text_tokens:
            # if term is a number in form ...d.d.. exp 230.3K - add to list
            if re.match(regex_pattern_for_num, word):
                new_words.append(word)
                continue
            # else - remove all punctuation from the term
            else:
                word = re.sub(regex_pattern_for_punctuation, '', word, flags=re.IGNORECASE)
                word = ''.join([i if ord(i) < 128 else '' for i in word])
                if word == '' or word == ' ':
                    continue

            new_words.append(word)
        text_tokens = new_words
##############################################################################################
        # find HASHTAGS
        # TODO: #whereIsKCR combined
        if "#" in text_tokens:
            index_list3 = [n for n, x in enumerate(text_tokens) if x == '#']
            for index in index_list3:
                if index + 1 < len(text_tokens):
                    if text_tokens[index + 1] != '#' and text_tokens[index + 1][0] != '@' and text_tokens[index + 1].find("#") == -1: #next word is not # and not @
                        if text_tokens[index + 1].find('_') == -1:  # not contains '_'
                            new_term = text_tokens[index] + text_tokens[index + 1]
                            text_tokens.append(new_term)
            for sign in range(len(index_list3)):  # deletes all '#' and the word after it from list
                rmv_index = text_tokens.index('#')
                if rmv_index + 1 < len(text_tokens) and text_tokens[rmv_index + 1] != '#'\
                        and text_tokens[rmv_index + 1][0] != '@' and text_tokens[rmv_index + 1].find("#") == -1:
                    word_val = text_tokens[rmv_index + 1]
                    if not word_val.isupper() and not word_val.islower() and word_val.find('_') == -1:  # split uppercase
                        list_of_words = re.findall('[A-Z][^A-Z]*', word_val)
                        for word in list_of_words:
                            text_tokens.append(word)
                    if word_val.find('_') != -1:  # split '_'
                        list_of_words = word_val.split('_')
                        new_word = "#"
                        for word in list_of_words:
                            new_word += word
                            text_tokens.append(word)  # appends each word
                        text_tokens.append(new_word)  # appends #word
                    if text_tokens[rmv_index + 1][0] != '@' and ((not word_val.isupper() and not word_val.islower()) or word_val.islower() or (word_val.find('_') != -1)): #TODO: delete #fuck_you
                        del text_tokens[rmv_index + 1]
                text_tokens.remove('#')

        #NUMBERS
        """numbers = []
        for item in text_tokens:  #([0-9]+[,.]+[0-9]+)  item.isnumeric() or item.isdigit() or item.isdecimal() or
            if re.findall("^\d+$|^[0-9]{1,3}([,.\/][0-9]{1,3}){0,6}$", item) and not re.search('[a-zA-Z]', item): #^\d+$|^[0-9]{1,3}([,.][0-9]{1,3})?$
                if item.find('-') == -1 and item.find('€') == -1 and item.find('£') == -1 and item.find(
                        '%') == -1 and item.find('¢') == -1 and item.find('~') == -1 and item.find('+') == -1 and item.find('/') == -1 and item.find("'") == -1:
                    if item.find(',') == -1:
                        numbers.append(item)
                    elif item.find(',') != -1 and re.findall("^([0-9]{1,3})(,[0-9]{3})*$", item):
                        numbers.append(item)
        if len(numbers) >0:
            print(numbers)
        for num in numbers:
            occur = num.count('.')
            if occur < 2:  # not a date
                rmv_index = text_tokens.index(num)
                to_append = True
                no_text = True
                if rmv_index + 1 < len(text_tokens):  # yes text
                    if text_tokens[rmv_index + 1] == "million" or text_tokens[rmv_index + 1] == "Million" or \
                            text_tokens[rmv_index + 1] == "M" or text_tokens[rmv_index + 1] == "m" or text_tokens[rmv_index + 1] == "MILLION":
                        if len(num) < 6:
                            fixed_num = re.sub("[^\d\.]", "", num)  # remove comma
                            new_num = self.parse_numbers(str(float(fixed_num) * 1000000))
                        else:
                            new_num = self.parse_numbers(num)
                        no_text = False
                        text_tokens[rmv_index + 1] = " "  # remove from list
                        text_tokens[rmv_index] = " "
                    if text_tokens[rmv_index + 1] == "billion" or text_tokens[rmv_index + 1] == "Billion" or \
                            text_tokens[rmv_index + 1] == "B" or text_tokens[rmv_index + 1] == "b" or text_tokens[rmv_index + 1] == "BILLION":
                        if len(num) < 9:
                            fixed_num = re.sub("[^\d\.]", "", num)  # remove comma
                            new_num = self.parse_numbers(str(float(fixed_num) * 1000000000))
                        else:
                            new_num = self.parse_numbers(num)
                        no_text = False
                        text_tokens[rmv_index + 1] = " "  # remove from list
                        text_tokens[rmv_index] = " "
                    if text_tokens[rmv_index + 1] == "thousand" or text_tokens[rmv_index + 1] == "Thousand" or \
                            text_tokens[rmv_index + 1] == "K" or text_tokens[rmv_index + 1] == "k" or text_tokens[rmv_index + 1] == "THOUSAND":
                        if len(num) < 4:
                            fixed_num = re.sub("[^\d\.]", "", num)  # remove comma
                            new_num = self.parse_numbers(str(float(fixed_num) * 1000))
                        else:
                            new_num = self.parse_numbers(num)
                        no_text = False
                        text_tokens[rmv_index + 1] = " "  # remove from list
                        text_tokens[rmv_index] = " "
                    if not no_text:
                        text_tokens[rmv_index + 1]
                if (rmv_index - 1 >= 0 and text_tokens[rmv_index - 1] == '$'): #yes $
                    if no_text:
                        if len(num) > 3:
                            text_tokens.append("$" + self.parse_numbers(num))
                        else:
                            text_tokens.append("$" + num)
                        text_tokens[rmv_index] = " "  # remove $ from list
                        text_tokens[rmv_index - 1] = " "
                    else:
                        text_tokens.append("$" + new_num)
                        text_tokens[rmv_index-1] = " "  # remove $ from list
                    to_append = False
                if to_append:  # no $
                    if no_text:
                        if len(num) > 3:
                            text_tokens.append(self.parse_numbers(num))
                            text_tokens[rmv_index] = " "  # remove num from list
                    else:
                        text_tokens.append(new_num)"""
##############################################################################################
        # add fractions
        text_tokens.extend(fractions_list)
##############################################################################################
        # remove stop_words
        text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        # print(text_tokens)
        # print(text_tokens_without_stopwords)
##############################################################################################
        # if stemmer
        if self.to_stem:
            stem_text_tokens_without_stopwords = []
            for token in text_tokens_without_stopwords:
                stem_token = Stemmer().stem_term(token)
                stem_text_tokens_without_stopwords.append(stem_token)
            #print(stem_text_tokens_without_stopwords)
            return stem_text_tokens_without_stopwords

        return text_tokens_without_stopwords

    def parse_url(self, url):
        url_tokens = re.split('[} |:// |":" |/ |: |, |" |{ |? |= |-]', url)
        website = [s for s in url_tokens if "www" in s]
        for web in website:
            new_web = web.split("www.")
            url_tokens.extend(new_web)
            url_tokens.append("www")
            url_tokens.remove(web)
        while "" in url_tokens:  #removes spaces
            url_tokens.remove("")
        return url_tokens

    def parse_numbers(self, str):
        fixed_str = re.sub("[^\d\.]", "", str)
        if len(fixed_str) > 22:  # num is too big
            return fixed_str
        num = float(fixed_str)
        magnitude = 0
        while abs(num) >= 1000:
            magnitude += 1
            num /= 1000.0
        num = floor((num * 1000)) / 1000
        new_num = '{}{}'.format('{:.3f}'.format(num).rstrip('0').rstrip('.'),
                                ['', 'K', 'M', 'B', 'T', 'Q', 'Q', 'SAF'][magnitude])
        return new_num

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]
        term_dict = {}

        # text tokenized
        tokenized_text = self.parse_sentence(full_text, tweet_id)
        if tokenized_text == []:
            tweet = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                             quote_url, {}, -1, 0, 0, self.capital_letter_dict,
                             self.term_dict)
            return tweet

        idx_in_tweet = 0
        for term in tokenized_text:
            if term.isdigit() or len(term) > 1:
                if not ( term == "!" or term == "##" or term == "#" or term == "~" or term == "#!" or term == "#%" or term == "~r"):
                    if term not in term_dict.keys():
                        term_dict[term] = [1, [idx_in_tweet]]  # 1->num of occur in tweet, idx_in_tweet-> place in tweet
                    else:
                        term_dict[term][0] += 1
                        term_dict[term][1].append(idx_in_tweet)
                    idx_in_tweet += 1

        doc_length = len(tokenized_text)  # after text operations.
        # url tokenized
        tokenized_url = self.parse_url(url)
        if len(tokenized_url) != 0:
            for term in tokenized_url:
                if term.find(".") != -1:
                    if not (term == "http" or term == "https" or term == "t.co" or term == "!"or term == "##" or term == "~r" or term == "#" or term == "~"):
                        if term not in term_dict.keys():
                            term_dict[term] = [1, [idx_in_tweet]]  # 1->num of occur in tweet, idx_in_tweet-> place in tweet
                        else:
                            term_dict[term][0] += 1
                            term_dict[term][1].append(idx_in_tweet)
                        idx_in_tweet += 1



        #print(term_dict)
        #print(tokenized_text)

        # find max_tf in each tweet
        max_tf = 0
        if len(term_dict) != 0:
            all_val = term_dict.values()
            max_tf = max(all_val)

        # find distinct_words
        distinct_words = 0
        if len(term_dict) != 0:
            all_keys = term_dict.keys()
            distinct_words = len(all_keys)

        tweet = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                         quote_url, term_dict, doc_length, max_tf, distinct_words, self.capital_letter_dict, self.term_dict)

        return tweet
