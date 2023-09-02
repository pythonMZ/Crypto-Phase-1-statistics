#Project 1 scraping part 1

from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import os


options = Options()
options.headless = True
driver = webdriver.Firefox(options = options)
driver.get('https://coinmarketcap.com/historical/20230825/')
t = driver.find_elements(By.CSS_SELECTOR, '.cmc-table-row a')
driver.quit


urls_list = []
for i in t:
    urls_list.append(i.get_attribute('href'))
first20 = urls_list[:60:3]
first20
others = urls_list[60:]
others
MainLinks = first20 + others

ranks =list(range(1,201))

names = []
for i in MainLinks:
    names.append(i[37:-1])

HistoricalLinks=[]
for i in MainLinks:
    HistoricalLinks.append(i+'historical-data/')

def get_url(url):
    res=requests.get(url)
    so= BeautifulSoup(res.text, "html.parser")
    return so
with ThreadPoolExecutor(max_workers=201) as pool:
    t=list(pool.map(get_url,MainLinks))

symbol=[]
for i in t:
    symbol.append(i.find(class_='sc-16891c57-0 cjeUNx base-text').text)

github_link=[]
for i in t:
    k=i.find_all(rel='nofollow noopener')
    d=0
    for j in k:
        if  'https://github.com/' in j.get('href'):
            github_link.append(j.get('href'))
            d=1
            break
    if d==0:
        github_link.append(None)


tags=[]
for i in t:
    k=i.find_all(class_='sc-16891c57-0 sc-b7faf77f-1 ctYAzo')
    m=[]
    for j in k:
        if j.text not in m:
            m.append(j.text)      
    tags.append(m)


df = pd.DataFrame({'Rank':ranks,
                   'Name':names,
                    'Symbol':symbol,
                    'MainLink':MainLinks,
                    'HistoricalLink':HistoricalLinks,
                    'Tags': tags,
                    'Github_link':github_link})
df.to_csv('CoinMarketCap.csv',index=False)




################ download csv at once


# def download(url):
#     option = Options()options=option
#     option.headless =True
#     driver_his=webdriver.Firefox()
#     driver_his.get(url)
#     time.sleep(1)
#     driver_his.find_element("xpath", '//*[@id="__next"]/div[2]/div[1]/div[2]/div/div/div/div[2]/div/div[1]/div/button[1]').click()
#     time.sleep(2)
#     driver_his.find_element("xpath", '//*[@id="tippy-1"]/div/div[1]/div/div/div[1]/div[2]/ul/li[5]').click()
#     time.sleep(2)
#     driver_his.find_element("xpath", '//*[@id="tippy-1"]/div/div[1]/div/div/div[2]/span/button').click()
#     time.sleep(2)
#     driver_his.find_element("xpath", '//*[@id="__next"]/div[2]/div[1]/div[2]/div/div/div/div[2]/div/div[1]/div/button[2]').click()
#     time.sleep(2)
#     driver_his.quit()
# with ThreadPoolExecutor(max_workers=201) as pool:
#     pool.map(download,HistoricalLinks)



# download csv one by one


for i in HistoricalLinks:
    option = Options()
    option.headless =True
    driver_his=webdriver.Firefox(options=option)
    driver_his.get(i)#
    time.sleep(7)
    driver_his.find_element("xpath", '//*[@id="__next"]/div[2]/div[1]/div[2]/div/div/div/div[2]/div/div[1]/div/button[1]').click()
    time.sleep(7)
    driver_his.find_element("xpath", '//*[@id="tippy-1"]/div/div[1]/div/div/div[1]/div[2]/ul/li[5]').click()
    time.sleep(7)
    driver_his.find_element("xpath", '//*[@id="tippy-1"]/div/div[1]/div/div/div[2]/span/button').click()
    time.sleep(7)
    driver_his.find_element("xpath", '//*[@id="__next"]/div[2]/div[1]/div[2]/div/div/div/div[2]/div/div[1]/div/button[2]').click()
    time.sleep(5)
    driver_his.quit()
