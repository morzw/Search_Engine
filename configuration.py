class ConfigClass:

    toStem = False

    def __init__(self):
        self.corpusPath = 'C:\\Users\\אורי בן-ארצי\\PycharmProjects\\Search_Engine\\testData'
        self.savedFileMainFolder = ''
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = False

        #print('Project was created successfully..')
    def get__corpusPath(self):
        return self.corpusPath
    def get__toStem(self):
        return self.toStem
    def get__savedFileMainFolder(self):
        return self.savedFileMainFolder
    def set__corpusPath(self, corpus_path):
        self.corpusPath = corpus_path
    def set__savedFileMainFolder(self, saved_file_main_folder):
        self.savedFileMainFolder = saved_file_main_folder
    def set__toStem(self, to_stem):
        self.toStem = to_stem