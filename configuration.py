class ConfigClass:
    def __init__(self):
        self.corpusPath = 'C:\\Users\\אורי בן-ארצי\\Google Drive\\לימודים\\ג1\\אחזור מידע\\Data\\date=07-08-2020'
        self.savedFileMainFolder = ''
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = False

        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath
