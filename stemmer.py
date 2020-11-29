import nltk
from nltk.stem import snowball


class Stemmer:
    def __init__(self):
      # self.stemmer = snowball.SnowballStemmer("english")
        self.stemmer = nltk.stem.porter.PorterStemmer()

    def stem_term(self, token):
        """
        This function stem a token
        :param token: string of a token
        :return: stemmed token
        """
        if token == "covid" or token == "Covid" or token == "covid-19" or token == "Covid-19" or token == "covid19" or token == "Covid19"\
            or token == "COVID" or token == "COVID19" or token == "COVID-19" or token == "covoid" or token == "CV19":
            return "covid"
        return self.stemmer.stem(token)
