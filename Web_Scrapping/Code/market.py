from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.firefox.options import Options
import pandas as pd



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
others = urls_list[60:]
MainLinks = first20 + others




final_list=[]



for i in MainLinks[:20]:
   tr=[]
   options = Options()
   options.headless = True
   drive= webdriver.Firefox(options = options) 
   drive.get(i)
   time.sleep(1)
   NAME=drive.find_element("xpath",'//*[@id="section-coin-markets"]/section/div/div[1]/div/h2').text
   drive.find_element("xpath",'//*[@id="section-coin-markets"]/section/div/div[3]/div[2]/div[2]/button').click()
   time.sleep(5)
   for j in range(1,11):
      path='/html/body/div[5]/div/div/div/div[2]/div[1]/table/tbody/tr[{}]'.format(j)
      time.sleep(2)
      l=drive.find_element("xpath",path).text.split('\n')
      time.sleep(1)
      l.insert(0,NAME)
      tr.append(l)
   drive.quit
   final_list.append(tr)

df_list=[]
for i in range(20):
    for j in range(10):
        df_list.append(final_list[i][j])

t=pd.DataFrame(df_list,columns=['Name','#','Exchange','Pair','Price, +2% Depth, -2% Depth, Volume (24h) ,Volume %','Confidence','Liquidity Score Updated'])
t.drop(['Confidence','Liquidity Score Updated'],axis=1,inplace=True)
t.to_excel('market.xlsx')
