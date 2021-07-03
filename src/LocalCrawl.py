import random

from bs4 import BeautifulSoup
import json
import pandas as pd
import requests

def crawlByPoiType(htmlhandle, poiType):
    global orderDatas
    soup = BeautifulSoup(htmlhandle, 'html.parser')
    n = 0
    if (poiType == 'meishi'):
        # print(soup)
        scriptList = soup.find_all("script")
        scriptIndexText = scriptList[15]
        scriptText = scriptIndexText.find(text=True)
        scriptText1 = scriptText.split(',"comHeader"')[0].split(' = ')[1] + '}'
        scriptJson = json.loads(scriptText1)
        jsonPoiLists = scriptJson['poiLists']
        jsonPoiInfos = jsonPoiLists['poiInfos']
        poiList = list(jsonPoiInfos)
        n = len(poiList)
        if (n == 0):
            return 0
        orderDatas = pd.DataFrame()
        for onePoiJson in poiList:
            poiId = onePoiJson['poiId']
            frontImg = onePoiJson['frontImg']
            title = onePoiJson['title']
            avgScore = onePoiJson['avgScore']
            allCommentNum = onePoiJson['allCommentNum']
            address = onePoiJson['address']
            # avgPrice=onePoiJson['avgPrice']
            print("{0},{1},{2},{3},{4},{5}".format(poiId, frontImg, title, avgScore, allCommentNum, address))
            row = [poiId, frontImg, title, avgScore, allCommentNum, address]
            print(row)
            orderDatas = orderDatas.append([row])
    else:
        orderDatas = pd.DataFrame()
        divList = soup.find_all(name="div", attrs={"class": "abstract-item clearfix"})
        n = len(divList)
        if (n == 0):
            return 0
        for perDiv in divList:
            aImageLable = perDiv.find(name='a', attrs={"class": "abstract-pic grey"})
            aTitleLabel = perDiv.find(name='a', attrs={"class": "item-title"})
            divScore = perDiv.find(name='div', attrs={"class": "item-eval-info clearfix"})
            spanCommentNum = perDiv.find(name='span', attrs={"class": "highlight"})
            divAddress = perDiv.find(name='div', attrs={"class": "item-site-info clearfix"})

            poiId = aTitleLabel.get('href').split('/')[4]
            divImage = aImageLable.find(name='div')
            imgLabel = divImage.find(name='img')
            frontImg = 'https:' + imgLabel.get('src')
            title = aTitleLabel.string
            avgScore = divScore.find('span').text.split('分')[0]
            allCommentNum = spanCommentNum.text.split('人评论')[0]
            address = divAddress.findAll('span')[1].text

            print("{0},{1},{2},{3},{4},{5}".format(poiId, frontImg, title, avgScore, allCommentNum, address))
            row = [poiId, frontImg, title, avgScore, allCommentNum, address]
            # print(row)
            orderDatas = orderDatas.append([row])
        # print(divList[1])
    orderDatas.to_csv('../data/Meituan/poi_data_{0}.csv'.format(poiType), index=False, header=None, mode='a')
    return n


def crawlByDetail(poiId, poiType, offset, proxy_pool):
    global url
    if poiType == 'meishi':
        url = 'https://www.meituan.com/meishi/api/poi/getMerchantComment'
    elif poiType == 'xiuxianyule' or poiType == 'yundongjianshen':
        url = 'https://i.meituan.com/xiuxianyule/api/getCommentList'
    elif poiType == 'shenghuo' or poiType == 'jiankangliren' or poiType == 'qinzi' or poiType == 'yiliao':
        url = 'https://www.meituan.com/ptapi/poi/getcomment'
    elif poiType == 'jiazhuang':
        url = 'https://www.meituan.com/jiazhuang/shop/review/list'
    else:
        return
    referer = 'https://www.meituan.com/{0}/{1}'.format(poiType, poiId)
    originUrl = 'https://www.meituan.com/{0}/{1}'.format(poiType, poiId)
    myCookie = 'uuid=d664f42127654932a05c.1625274199.1.0.0; _lxsdk_cuid=17a69e42e789-0de26284296751-6373264-144000-17a69e42e79c8; __mta=119845559.1625274205976.1625274205976.1625274205976.1; ci=60; mtcdn=K; u=106783540; n=茗之伤; lt=McUCcGgWSDg8GuDaxdZlTO6xeFoAAAAA7w0AAEiX6cNwXG1kVHgpIpBD9s5lrr-jUNZpvBP-vbZ5Zoc22FkbGlf7kL4mkF6iXrjybw; mt_c_token=McUCcGgWSDg8GuDaxdZlTO6xeFoAAAAA7w0AAEiX6cNwXG1kVHgpIpBD9s5lrr-jUNZpvBP-vbZ5Zoc22FkbGlf7kL4mkF6iXrjybw; token=McUCcGgWSDg8GuDaxdZlTO6xeFoAAAAA7w0AAEiX6cNwXG1kVHgpIpBD9s5lrr-jUNZpvBP-vbZ5Zoc22FkbGlf7kL4mkF6iXrjybw; lsu=; token2=McUCcGgWSDg8GuDaxdZlTO6xeFoAAAAA7w0AAEiX6cNwXG1kVHgpIpBD9s5lrr-jUNZpvBP-vbZ5Zoc22FkbGlf7kL4mkF6iXrjybw; unc=茗之伤; _lxsdk=17a69e42e789-0de26284296751-6373264-144000-17a69e42e79c8; rvct=60; client-id=69d587b7-e6da-4752-821c-d8bcd8288d10; _hc.v=77289b21-a426-0acf-376d-e911a8dd82cc.1625274646; lat=36.064802; lng=120.315526; wed_user_path=6700|0; locallng=120.132428; geoType=wgs84; locallat=35.977603; Hm_lvt_dbeeb675516927da776beeb1d9802bd4=1625276079,1625276100; Hm_lpvt_dbeeb675516927da776beeb1d9802bd4=1625276100; _lx_utm=utm_source=Baidu&utm_medium=organic; firstTime=1625276569643; _lxsdk_s=17a69e42e79-49e-551-427||201'
    header = {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Referer': referer,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
        'Host': 'www.meituan.com',
        'Cookie': myCookie.encode('utf-8').decode('latin1')
    }
    global params
    if poiType == 'meishi':
        params = {
            'uuid': '0db55011ce4444799d10.1625141968.1.0.0',
            'platform': '1',
            'partner': '126',
            'originUrl': originUrl,
            'riskLevel': '1',
            'optimusCode': '10',
            'id': poiId,
            'userId': '106783540',
            'offset': offset,
            'pageSize': '10',
            'sortType': '1'
        }
    elif poiType == 'xiuxianyule' or poiType == 'yundongjianshen':
        params = {
            'poiId': poiId,
            'offset': offset,
            'pageSize': '10',
            'sortType': '1',
            'mode': '0',
            'starRange': '10,20,30,40,50'
        }
    elif poiType == 'shenghuo' or poiType == 'jiankangliren' or poiType == 'qinzi' or poiType == 'yiliao':
        params = {
            'id': poiId,
            'offset': offset,
            'pageSize': '10',
            'mode': '0',
            'sortType': '1',
        }
    elif poiType == 'jiazhuang':
        params = {
            'sortType': '1',
            'pageNo': offset / 10 + 1,
            'pageSize': '10',
            'shopId': poiId
        }
    try:
        proxy_ip = random.choice(proxy_pool)
        proxies = {'https': proxy_ip}
        response = requests.get(url, headers=header, params=params, proxies=proxies)
        response_json = response.text

        return response.text
    except Exception as e:
        print(e)


if __name__ == '__main__':
    # path = 'D:/课程/个人论文/毕业论文-new/数据集/Meituan/pages/xxyl1.html'
    # htmlfile = open(path, 'r', encoding='utf-8')
    # htmlhandle = htmlfile.read()
    # crawlByPoiType(htmlhandle,'xiuxianyule')
    iptxt = pd.read_csv('../data/Meituan/ips.txt',header=None)
    print(crawlByDetail(1541084094, 'meishi', 0, list(iptxt[0])))
