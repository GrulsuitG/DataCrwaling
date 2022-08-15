from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

#driver option setting
def option_setting(headless = True):
    options = webdriver.ChromeOptions()
    if headless : 
        options.add_argument('headless') # 크롬 띄우는 창 없애기 
    options.add_argument('window-size=1920x1080') # 크롬드라이버 창크기 
    options.add_argument("disable-gpu") #그래픽 성능 낮춰서 크롤링 성능 쪼금 높이기 
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36") # 네트워크 설정 
    options.add_argument("lang=ko_KR") # 사이트 주언어 
    options.add_experimental_option('excludeSwitches', ['enable-logging']) #module find error handle
    return options

def make_driver(headless = True):
    options = option_setting(headless)
    return webdriver.Chrome(chrome_options=options)