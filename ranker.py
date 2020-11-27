class Ranker:

    def __init__(self):
        pass

    @staticmethod
    def rank_relevant_doc(list_of_relevant_tweet):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        return list_of_relevant_tweet

    @staticmethod
    def retrieve_top_k(relevant_doc, k=5):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return relevant_doc[:k]
