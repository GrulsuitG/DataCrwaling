from datetime import timedelta, datetime
import logging
import os
import time
import requests
import re
import sys

from bs4 import BeautifulSoup
import threading

import DB as db
import elastic as ela

threadNum = 4


# title 길이가 길 때 잘라내는 부분
def trim_msg(str_msg, int_max_len=100, encoding='utf-8'):
    try:
        return str_msg.encode(encoding)[:int_max_len].decode(encoding)
    except UnicodeDecodeError:
        try:
            return str_msg.encode(encoding)[:int_max_len - 1].decode(encoding)
        except UnicodeDecodeError:
            return str_msg.encode(encoding)[:int_max_len - 2].decode(encoding)


# ews crawling main 함수
def news_crawling(keyword, start_date, end_date, lock):
    keyword = '"' + keyword[1].replace(' ', '+') + '"'

    if start_date == '' and end_date == '':
        url = 'https://search.naver.com/search.naver?where=news&query={}&sm=tab_opt&&pd=2'
        target_url = url.format(keyword)
    else:
        url = 'https://search.naver.com/search.naver?where=news&query={}&sm=tab_opt&pd=3&ds={}&de={}&start=1'
        target_url = url.format(keyword, start_date, end_date)

    # 정해진 기간의 날짜의 뉴스를 모두 크롤링
    while not target_url is None:
        soup = get_bfsoup(target_url)
        links = get_naver_news_link(soup)
        get_news_info(links, keyword, lock)

        try:
            next = soup.find('a', {'class': 'btn_next'})['href']
        except:
            break

        target_url = 'https://search.naver.com/search.naver' + next


# url로부터 html을 가져오는 method
def get_bfsoup(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36 Edg/90.0.818.66'}
    _html = ""
    resp = requests.get(url, headers=headers)
    while True:
        if resp.status_code == 200:
            _html = resp
            break
        else:
            time.sleep(1)

    soup = BeautifulSoup(_html.content, 'html.parser')
    return soup


# 네이버 뉴스를 포함하는 link를 골라 return 해주는 method
def get_naver_news_link(soup):
    links = []
    for group in soup.find_all('div', 'info_group'):
        for data in group.find_all('a'):
            l = str(data.get('href'))
            if 'naver' in l:
                links.append(l)
    return links


# 링크들을 순회하면서 news crawling 을 invoke 하는 method
def get_news_info(links, keyword, lock):
    es = ela.connect()
    for link in links:
        soup = get_bfsoup(link)
        data = get_news_data(soup, link)
        if data is None:
            continue
        else:
            body = {
                "keyword": keyword[1],
                "title": data[0],
                "htmlcontents": data[1],
                "contents": data[2],
                "press": data[3],
                "writeDate": data[4],
                "reporter": data[5],
                "link": link
            }
            ela.insert(es, "news", body)
            time.sleep(0.5)
    es.close()


# 링크들로부터 직접 data 를 불러오는 method
def get_news_data(soup, link):
    try:
        # 기사 제목
        title = soup.find('h3').text
        if len(title.encode('utf-8')) > 100:
            title = trim_msg(title)

        # 본문 및 html
        # 사진 캡션을 포함한 text를 추출해 return
        htmldata = soup.find('div', {'id': 'articleBodyContents'})
        contents = htmldata.text.strip()
        htmlcontents = str(htmldata)

        # 언론사 정보
        mediadata = soup.find('div', attrs={'class': 'press_logo'})
        press = mediadata.find('img')['alt']

        # 기사입력일시
        writeDate = soup.find('span', attrs={'class': 't11'}).text
    except Exception as e:
        logging.warning('Raise a Exception')
        logging.warning('%s', link)
        return None

    # 기자명
    reporter = ''
    reporterdata = soup.find('p', attrs={'class': 'b_text'})
    if reporterdata != None:
        reporterdata = reporterdata.text.strip()
        reporter = reporterdata[:3]

        #기자 이메일 주소
        pattern = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+(\.[a-zA-Z]{2,4}))"
        reporter_mail = re.search(pattern, reporterdata).group()

        inParam = [title
            , htmlcontents
            , contents
            , press
            , writeDate
            , reporter]

        return inParam


# 단일 process crawling
def normal_crawling(keywordList, start_date, end_date, lock=0):
    for keyword in keywordList:
        print(keyword[1] + " Crawling start")
        logging.info("(%s ~ %s) %s Crawling start", start_date, end_date, keyword[1])
        news_crawling(keyword, start_date, end_date, lock)

        # 한달치 데이터를 크롤링하면 Code를 N로 변경
        if keyword[4] == 'Y':
            myPageDB = db.MyDB()
            myPageDB.setPageDB()

        logging.info("%s Crawling finish", keyword[1])
        print(keyword[1] + " Crawling finish")


# multi thread crawling
def threading_crawling(stockList, myPageDB, start_date='', end_date=''):
    lock = threading.Lock()
    stockLen = len(stockList)
    howMany = int(stockLen / threadNum)

    for i in range(threadNum):
        num = i
        keywordList = []
        while num < stockLen:
            keywordList.append(stockList[num])
            num += threadNum
        crawling_thread = threading.Thread(target=normal_crawling,
                                           args=(keywordList, myPageDB, start_date, end_date, lock))
        crawling_thread.start()


def crawling_to_ela(s_date, e_date, stockList):
    es = ela.connect()
    ela.create_index(es, "news")
    es.close()

    normal_crawling(stockList, s_date, e_date)

    # multi thread 실행 방식
    # mainThread = threading.currentThread()
    # threading_crawling(stockList, myPageDB, s_date, e_date)
    # for thread in threading.enumerate():
    #     # Main Thread를 제외한 모든 Thread들이 
    #     # 카운팅을 완료하고 끝날 때 까지 기다린다.
    #     if thread is not mainThread:
    #         thread.join()


def ela_to_DB(keywordList):
    es = ela.connect()
    ela.create_index(es, "news")

    myPageDB = db.MyDB()
    myPageDB.setPageDB()

    for keyword in keywordList:
        keywordID = keyword[2]
        excludeList = myPageDB.excludeKeywordList(keywordID)
        logging.info("%s  DB store start", keyword[1])
        # 검색 결과에 따라 DB에 저장
        include = ela.get_NonExclude(es, "news", keyword[1], excludeList)
        exclude = ela.get_exclude(es, "news", keyword[1], excludeList)

        logging.info("%s 포함 데이터 저장", keyword[1])

        for res in include['hits']['hits']:
            data = res['_source']
            cnt += myPageDB.insPageNewsByDict(data, "include")

        logging.info("%s 제외 데이터 저장", keyword[1])
        for res in exclude['hits']['hits']:
            data = res['_source']
            myPageDB.insPageNewsByDict(data, "exclude")

        logging.info("%s  DB store finish", keyword[1])

    print("DB store finish")


def main(s_date='', e_date='', crawling=True, saveToDB=True):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    log_dir = '{}/logs'.format(current_dir)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    file_name = './logs/' + time.strftime('%Y%m%d') + '.log'
    logger = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_Handler = logging.FileHandler(
        filename=file_name, mode='a', encoding='utf-8'
    )
    file_Handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_Handler)
    myPageDB = db.MyDB()
    myPageDB.setPageDB()

    # 종목코드 가져오기 
    keywordList = myPageDB.selKeywordList()
    if crawling:
        crawling_to_ela(s_date, e_date, keywordList)
    if saveToDB:
        ela_to_DB(keywordList)

    es = ela.connect()
    ela.delete_index(es, "news")
    logging.info("elastic index delete")

    logging.info("프로그램 종료")


if __name__ == "__main__":

    argument = sys.argv

    if len(argument) == 1 or argument[1] == '1':
        today = datetime.today().strftime("%Y.%m.%d")
        monthago = (datetime.today() - timedelta(days=31)).strftime("%Y.%m.%d")
        main(monthago, today)
    elif argument[1] == '2':

        p = re.compile('\d{4}.\d{2}.\d{2}')
        try:
            s_date = argument[2]
            e_date = argument[3]
        except:
            print("시작 날짜와 끝 날짜를 입력해야 합니다.")
            exit()

        if p.match(s_date) is None:
            print("잘못된 형식입니다. YYYY.MM.DD로 입력하세요")
            exit()
        if p.match(e_date) is None:
            print("잘못된 형식입니다. YYYY.MM.DD로 입력하세요")
            exit()

        main(s_date, e_date)

    elif argument[1] == '3':
        today = datetime.today().strftime("%Y.%m.%d")
        main(today, today)
    else:
        print("잘못된 입력")
