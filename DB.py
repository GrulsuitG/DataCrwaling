import pyodbc
import json
import logging

class MyDB:
    def readDBInfo(self):
        with open('config.json', 'r') as f:
            config = json.load(f)
        
        return config
    
    def inputDBInfo(self):
        database = input("database : ")
        username = input("username : ")
        password = input("password : ")

        config = {
            "database" : database,
            "username" : username,
            "password" : password
        }
        
        return config
        
    def setPageDB(self):
        
        config = self.readDBInfo()
        
        self.cnxn = pyodbc.connect(host='localhost',
                                port=3306,
                                user=config['username'],
                                passwd=config['password'],
                                db=config['database'],
                                charset='utf8')
        
        self.cursor = self.cnxn.cursor()

        print("DB Connect!")

    def insPageNews(self, inParam):

        self.cursor.execute("""
            SELECT Keyword
            FROM News
            WHERE  Keyword = ? AND Title = ? """,
        inParam[0], inParam[1])
        rows = self.cursor.fetchall()
        
        if len(rows) == 0 :
    
            self.cursor.execute("""
            INSERT INTO News (  Keyword, 
                                Title,
                                HtmlContents,
                                Contents,
                                Press,
                                WriteDate,
                                Writer,
                                NewsURL) 
            VALUES (?,?,?,?,?,?,?,?)""",
            inParam[0], inParam[1], inParam[2], inParam[3], inParam[4], inParam[5], inParam[6], inParam[7], inParam[8], inParam[9]
            )
            self.cnxn.commit()
            logging.info('[news] %s/%s input in db', inParam[1], inParam[7])
            return 1
        else :
            logging.info('[news] %s/%s is already exists', inParam[1], inParam[7])
            return 0
            
    def insPageNewsByDict(self, inParam, flag):
        
        searchQuery = """
                SELECT Keyword
                FROM {}
                WHERE  Keyword = ? AND Title = ?
        """
        insertQuery = """
            INSERT INTO {} (Keyword,
                            Title
                            HtmlContents
                            Contents
                            Press
                            WriteDate
                            Writer 
                            NewsURL)
            VALUES (?,?,?,?,?,?,?,?)
            """
        if flag == "include":
            searchQuery = searchQuery.format("NewsInclude")
            insertQuery = insertQuery.format("NewsInclude")
        else:
            searchQuery = searchQuery.format("NewsExclude")
            insertQuery = insertQuery.format("NewsExclude")
        
        self.cursor.execute(searchQuery, inParam['keyword'], inParam['title'])
        rows = self.cursor.fetchall()
        
        if len(rows) == 0 :
            self.cursor.execute(insertQuery,
            inParam['keyword'], inParam['title'], inParam['htmlcontents'], inParam['contents'], inParam['press'], inParam['writeDate'], inParam['reporter'], inParam['link'], inParam['type']
            )
            self.cnxn.commit()
            logging.info('[news][%s] %s/%s input in db', inParam['writeDate'], inParam['title'], inParam['link'])
        else :
            logging.info('[news][%s] %s/%s is already exists', inParam['writeDate'], inParam['title'], inParam['link'])
            
    def insYoutube(self, inParam):
        self.cursor.execute("""
            SELECT YoutubeURL, WriteDate, ViewCount 
            FROM Youtube
            WHERE  YoutubeURL = ?""",
        inParam[5])
        row = self.cursor.fetchone()
        if row is None:
            self.cursor.execute("""
            INSERT INTO Youtube (Keyword,
                                 Title,
                                 Contents,
                                 Press,
                                 WriteDate,
                                 YoutubeURL,
                                 imgURL,
                                 ViewCount) 
            VALUES (?,?,?,?,?,?,?,?)""",
            inParam[0], inParam[1], inParam[2], inParam[3], inParam[4], inParam[5], inParam[6], inParam[7]) 
            self.cnxn.commit()        
            logging.info('[youtube] %s/%s input in db', inParam[1], inParam[5])
            return True
        elif row[1] != inParam[4] or row[2] != inParam[8]:
            self.cursor.execute("""
            UPDATE Youtube
            SET ViewCount = ?,
                WriteDate = ?
            WHERE YoutubeURL = ? """, 
            inParam[7], inParam[4], inParam[5])
            self.cnxn.commit()        
            
            logging.info('[youtube] %s/%s update ViewCount %s -> %s WriteDate %s -> %s', inParam[1], inParam[5], row[2], inParam[7], row[1], inParam[4])
        else :
            logging.info('[youtube] %s/%s is already exist', inParam[1], inParam[5])
        
        return False
            
    def selKeywordList(self):
        self.cursor.execute("SELECT * FROM Target")
        stockList = []
        row = self.cursor.fetchone()

        while row: 
            stockList.append(row)
            row = self.cursor.fetchone()

        return stockList
    
    
    def excludeKeywordList(self, targetID):
        self.cursor.execute("SELECT KeywordName FROM ExcludeKeyword WHERE targetID = {}".format(targetID))
        excludeList = []
        row = self.cursor.fetchone()

        while row: 
            excludeList.append(row[0])
            row = self.cursor.fetchone()

        return excludeList