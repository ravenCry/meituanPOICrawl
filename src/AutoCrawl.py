from selenium import webdriver
from bs4 import BeautifulSoup
import json
import pandas as pd
import time
import LocalCrawl as lc
import random
poi_types=['meishi','xiuxianyule','shenghuo','jiankangliren','jiehun','qinzi','yundongjianshen','jiazhuang','yiliao']
login_url='https://passport.meituan.com/account/unitivelogin'
F0T_url='https://www.meituan.com/error/403'

#获得浏览器驱动
def get_driver():
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome_driver = r"chrome//chromedriver.exe"
    driver = webdriver.Chrome(executable_path=chrome_driver,options=options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
    Object.defineProperty(navigator, 'webdriver', {
       get: () => undefined
     })
   """
    })

    driver.implicitly_wait(10)
    return driver

#登录 防止token过期
def login(driver):
    driver.get(login_url)
    elem = driver.find_element_by_xpath("//*[@placeholder='手机号']")
    elem.send_keys("13515425381")
    time.sleep(3)
    elem = driver.find_element_by_xpath("//*[@placeholder='密码']")
    elem.send_keys("0629DaQian")
    time.sleep(3)
    driver.find_element_by_xpath("//*[@class='btn']").click()
    time.sleep(3)

#验证码填写???
def verifyCode(driver):
    print('verifyCode')

#根据兴趣点类型挖掘兴趣点
def getPoiByPoiType(driver, startPage, startType=1):
    url_head='https://qd.meituan.com'
    for j in range(startType-1,len(poi_types)):
        poi_type=poi_types[j]
        url_current=url_head+'/'+poi_type
        poiDatas=pd.DataFrame()
        poiDatas=poiDatas.append([['poi_id','front_img','title','avg_score','all_comment_num','address']])
        poiDatas.to_csv('../data/Meituan/poi_data_{0}.csv'.format(poi_type),index=False,header=None,mode='a')
        i=startPage
        while(True):
            try:
                url_per_current=(url_current+'/pn{0}/').format(i)
                time.sleep(2)
                driver.get(url_per_current)
                url_real=driver.current_url.split('?')[0]
                print(url_real)
                if(url_real==login_url or url_real==F0T_url):
                    login(driver)
                else:
                    for j in range(0,20):
                        time.sleep(2)
                        driver.execute_script('window.scrollTo(1000,(document.body.scrollHeight/20)*{0});'.format(j))
                    htmlhandle=driver.page_source
                    n = lc.crawlByPoiType(htmlhandle,poi_type)
                    if(n == 0):
                        break
                    time.sleep(3)
                    i+=1
            except:
                break
            print('{0} over.'.format(poi_type))
    print('all over.')

#根据兴趣点csv挖掘评论
def getPoiCommentByPoiCsv(current_poi_type_index=0,current_poi_id_index=0,current_offset=0):
    #['poi_id','front_img','title','avg_score','all_comment_num','address']
    #读代理ip池
    iptxt = pd.read_csv('../data/Meituan/ips.txt',header=None)
    proxy_pool=list(iptxt[0])
    for i in range(current_poi_type_index,9):
        data = pd.read_csv('../data/Meituan/poi_data_{0}.csv'.format(poi_types[i]))
        poi_id_list = list(data['poi_id'])
        poi_id_n = len(poi_id_list)
        for j in range(current_poi_id_index,poi_id_n):
            offset = 0
            if i==current_poi_type_index and j==current_poi_id_index:
                offset = current_offset
            while True:
                jsonStr=lc.crawlByDetail(poi_id_list[j],poi_types[i],offset,proxy_pool)
                print(jsonStr)
                if not lc.analysePoiCommentJson(poi_types[i], jsonStr):
                    return
                print("crawled position:(poi_type_index){0},(poi_id_index){1},(offset){2}".format(i,j,offset))
                offset += 10

#自动获取数据
def autoGetData(driver,startPage,startType=1):
    #getPoiByPoiType(driver,startPage,startType)
    getPoiCommentByPoiCsv()

if __name__ == '__main__':
    #driver = get_driver()
    #autoGetData(driver,1)
    getPoiCommentByPoiCsv(0,0,0)