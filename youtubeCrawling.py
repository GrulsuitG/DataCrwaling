import requests
import sys
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import logging
import logging.handlers
import threading
import os

import driver as dv
import DB as db

threadNum = 5

#page down method
def pagedown(driver):
    
    no_of_pagedowns = 2
    elem = driver.find_element_by_tag_name("body") 
    while no_of_pagedowns: 
        elem.send_keys(dv.Keys.PAGE_DOWN) 
        time.sleep(1) 
        no_of_pagedowns -= 1

#title 길이가 길 때 잘라내는 부분
def trim_msg(str_msg, int_max_len=200, encoding='utf-8'):
    try:
        return str_msg.encode(encoding)[:int_max_len].decode(encoding)
    except UnicodeDecodeError:
        try:
            return str_msg.encode(encoding)[:int_max_len-1].decode(encoding)
        except UnicodeDecodeError:        
            return str_msg.encode(encoding)[:int_max_len-2].decode(encoding)

def youtube_crawling(query, myPageDB, mode, lock):
    driver = driver_setting(query[1], mode)
    
    times = 0
    while times < 5 :
        pagedown(driver)
        
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all('ytd-video-renderer', {'class' : 'style-scope ytd-item-section-renderer'})
        
        for link in links[times*5:(times+1)*5]: 
            try:
                title = link.find('a', {'id' : 'video-title'} )['title']
                if len(title.encode('utf-8')) > 200 :
                    title = trim_msg(title)
                
                youtubeID = link.find('a', {'id' : 'video-title'}).get('href')   
                url = "www.youtube.com" + youtubeID
                imageUrl = "https://i.ytimg.com/vi/"+ youtubeID.replace("/watch?v=","")+"/hqdefault.jpg"
                
                view = link.find('a', {'id' : 'video-title'}).get('aria-label').split()[-1]
                view = view[:-1].replace(',','')
                if view.isdigit():
                    view = int(view)
                else :
                    view = 0
                
                channel = link.find('a', {'class' : 'yt-simple-endpoint style-scope yt-formatted-string'}).get_text()
                
                contents = link.find('yt-formatted-string', {'class' : 'metadata-snippet-text style-scope ytd-video-renderer'})
                if contents != None:
                    contents = contents.get_text()
                
                date = link.select_one('#metadata-line > span:nth-child(2)')
                if date != None:
                    date = date.get_text()
            
                inParam = [ query[1] #Keyword
                        , title #title
                        , contents # Contents
                        , channel # press
                        , date # writeDate
                        , url # youtubeURL
                        , imageUrl # imageURL
                        , view #view
                        ]
                
                cnt += myPageDB.insYoutube(inParam)
                
            except Exception as e:
                logging.warning('%s Raise a Exception %s', title, str(e))
            
        times += 1
    driver.close()   
    driver.quit()
    
def driver_setting(query, mode):
    driver = dv.make_driver()
    query = '"' + query + '"'
    url = 'https://www.youtube.com/results?search_query='+query
    driver.get(url)
    # 필터버튼 클릭
    # driver.find_element_by_xpath('/html/body/ytd-app/div/ytd-page-manager/ytd-search/div[1]/ytd-two-column-search-results-renderer/div/ytd-section-list-renderer/div[1]/div[2]/ytd-search-sub-menu-renderer/div[1]/div/ytd-toggle-button-renderer/a/tp-yt-paper-button').click();
    #이번 달
    # if mode == 1:
    #     driver.find_element_by_xpath('/html/body/ytd-app/div/ytd-page-manager/ytd-search/div[1]/ytd-two-column-search-results-renderer/div/ytd-section-list-renderer/div[1]/div[2]/ytd-search-sub-menu-renderer/div[1]/iron-collapse/div/ytd-search-filter-group-renderer[1]/ytd-search-filter-renderer[4]/a/div/yt-formatted-string').click()
    # #오늘
    # if mode == 2:
    #     driver.find_element_by_xpath('/html/body/ytd-app/div/ytd-page-manager/ytd-search/div[1]/ytd-two-column-search-results-renderer/div/ytd-section-list-renderer/div[1]/div[2]/ytd-search-sub-menu-renderer/div[1]/iron-collapse/div/ytd-search-filter-group-renderer[1]/ytd-search-filter-renderer[2]/a/div/yt-formatted-string').click()
        
    time.sleep(1)
    return driver

def normal_crawling(KeywordList, myPageDB, mode, lock = 0):
    for Keyword in KeywordList:
        logging.info('%s Crawling start', Keyword[1])
        youtube_crawling(Keyword, myPageDB, mode, lock)
        logging.info('%s Crawling finish', Keyword[1])
        
        
# multi thread crawling
def threading_crawling(keywordList, myPageDB, mode):
    lock = threading.Lock()
    
    keywordLen = len(keywordList)
    howMany = int(keywordLen / threadNum)
    
    for i in range(threadNum):
        num = i
        targetList = []
        while num < keywordLen : 
            targetList.append(keywordList[num])
            num += threadNum
        
        crawling_thread = threading.Thread(target=normal_crawling, args=(targetList, myPageDB, mode,  lock ))
        crawling_thread.start()
    
def main(mode):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    log_dir = '{}/logs'.format(current_dir)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    file_name = './logs/'+time.strftime('%Y%m%d')+'.log'
    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_Handler = logging.FileHandler(
        filename=file_name, mode ='a', encoding='utf-8'
    )
    file_Handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_Handler)
    
    
    myPageDB = db.MyDB()
    myPageDB.setPageDB()
    
    keywordList = myPageDB.selKeywordList()
    try:
        normal_crawling(keywordList, myPageDB, mode)
    except Exception as e:
        logging.error("Error 발생" + str(e))
    
    # mainThread = threading.currentThread()
    # threading_crawling(stockList, myPageDB, mode)
    # for thread in threading.enumerate():
    #     # Main Thread를 제외한 모든 Thread들이 
    #     # 카운팅을 완료하고 끝날 때 까지 기다린다.
    #     if thread is not mainThread:
    #         thread.join()

if __name__ == "__main__":
    
    argument = sys.argv
    
    #이번 달
    if len(argument) == 1 or argument[1] == '1':
        mode = 1
        main(mode)
    #오늘
    elif argument[1] == '2':
        mode = 2
        main(mode)
    else:
        print("잘못된 입력")
    