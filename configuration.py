class ConfigClass:
    def __init__(self):
        self.corpusPath = 'C:\\הנדסת מערכות מידע\\שנה ג\\סמסטר א\\אחזור מידע\\חלק א\\Data\\Data\\date=07-08-2020'
        self.savedFileMainFolder = ''
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = False

        print('Project was created successfully..')

    def get__corpusPath(self):
        return self.corpusPath
