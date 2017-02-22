# -*- coding: utf-8 -*-

import urllib
from urllib.request import urlopen,Request
from urllib.parse import quote
from bs4 import BeautifulSoup
import random
import pymysql
from datetime import datetime
import time
import socket
import threading


hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]

proxys_src = []
proxys = []


#=========================setup a database, only execute in 1st running=================================
def database_init(dbflag='local'):
     if dbflag=='local':
         conn = pymysql.connect(user='lianjia', passwd='lj123456', db='lianjiaHouse',host='raspberrypi',charset='utf8')
     else:
         conn = pymysql.connect(user='qdm1944', passwd='password', db='qdm1944_db',host='qdm1944.my3w.com')

     # 创建表:

     conn.query('create table if not exists cellInfo (obtainDate date,cellID varchar(50) primary key,name varchar(100),city varchar(10),\
                 region varchar(20),zone varchar(20),evenSalePrice int,onSaleNumber int,getDetailSign int,progressID int)')

     conn.query('create table if not exists cellInfo_H (obtainDate date,cellID varchar(50),name varchar(100),city varchar(10),\
                 region varchar(20),zone varchar(20),evenSalePrice int,onSaleNumber int,getDetailSign int,progressID int)')

     conn.query('create table if not exists onSaleHouseInfo (houseID varchar(50) primary key,link varchar(200),cellID varchar(50),years varchar(20),\
                 type varchar(50),square float,direction varchar(20),floor varchar(50),roughDate varchar(50),obtainDate datetime,totalPrice float,\
                 unitPrice int,focusNumber int,visitNumber int,progressID int)')

     conn.query('create table if not exists onSaleHouseInfo_H (houseID varchar(50),link varchar(200),cellID varchar(50),years varchar(20),\
                 type varchar(50),square float,direction varchar(20),floor varchar(50),roughDate varchar(50),obtainDate datetime,totalPrice float,\
                 unitPrice int,focusNumber int,visitNumber int,progressID int)')

     conn.query('create table if not exists city (shortName varchar(10),fullName varchar(50),url varchar(100))') 
     
     conn.query('create table if not exists region (city varchar(10), name varchar(20), urlName varchar(50))') 
     
     conn.query('create table if not exists grabPlan ( ID int primary key AUTO_INCREMENT,type int,city varchar(10),region varchar(20),\
                 cellID varchar(50),onProgress int,createTime datetime)')
 
     conn.query('create table if not exists grabPlanProgress (ID int primary key auto_increment,planID int,city varchar(10),region varchar(20),\
                 cellID varchar(50),totalCellPage int,validCellPage int,getCellPage int,totalCellNumber int,getCellNumber int,startTime datetime,\
                 endTime datetime)')

     conn.commit()
     
     return conn

def cellinfo_insert_mysql(conn,info_dict):

    cellinfo_list = [u'obtainDate', u'cellID', u'name', u'region', u'zone', u'evenSalePrice', u'onSaleNumber', u'progressID',u'city']

    t = []
    for il in cellinfo_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t = tuple(t)  # for houseinfo

    cursor = conn.cursor()
    try:
        cursor.execute('insert into cellInfo (obtainDate, cellID,name,region,zone,evenSalePrice,onSaleNumber,getDetailSign,progressID,city) \
         values (%s,%s,%s,%s,%s,%s,%s,0,%s,%s)', t)
        conn.commit()
    except Exception as e:
        print (e)
    cursor.close()

def cellinfo_getupdate_mysql(conn,cellID):
    cursor = conn.cursor()
    try:
        cursor.execute('update cellInfo set getDetailSign = 1 where cellID = %s', cellID)
        conn.commit()
    except Exception as e:
        print (e)
    cursor.close()

#-- for a single houseinfo, insert/update the mysql db
def houseinfo_insert_mysql(conn,info_dict):

    houseinfo_list = [u'houseID',u'link',u'cellID',u'years',u'type',u'square',u'direction',u'floor',\
               u'roughDate',u'obtainDate', u'totalPrice',u'unitPrice',u'focusNumber',u'visitNumber',u'progressID']

    t1=[]
    for il in houseinfo_list:
        if il in info_dict:
            t1.append(info_dict[il])
        else:
            t1.append('')
    t1=tuple(t1)    # for houseinfo

    cursor = conn.cursor()
    try:
        cursor.execute('insert into onSaleHouseInfo (houseID,link,cellID,years,type,square,direction,floor,\
                      roughDate,obtainDate,totalPrice,unitPrice,focusNumber,visitNumber,progressID) values \
                      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', t1)
#        trigger_notify_email(info_dict,'newhouse')
        conn.commit()
    except Exception as e:
        print (e)
    cursor.close()


#读取grabPlan表，将里面onProgress=0的抓取计划转换成抓取进度记录grabPlanProgress，然后设置grabPlan中的onProgress字段为1。
def convert_plan_to_progress(conn):
    cursor = conn.cursor()
    try:
        cursor.execute('insert into grabPlanProgress (planID,city,region) \
                       select b.id, a.city, a.urlName from region a,grabPlan b where b.type=1 and b.city=a.city and b.onProgress=0')
        cursor.execute('insert into grabPlanProgress (planID,city,region) \
                       select b.id, b.city, b.region from grabPlan b where b.type=2 and b.onProgress=0')
        cursor.execute('insert into grabPlanProgress (planID,city, region,cellID) \
                       select b.id,b.city,b.region,b.cellID from grabPlan b where b.type=3 and b.onProgress=0')
        cursor.execute('update grabPlan set onProgress=1')
        conn.commit()
    except Exception as e:
        print(e)
    cursor.close()

def run_planProgress(conn):
    print ('run_planProgress')
    cursor = conn.cursor()
    try:
        cursor.execute('select ID from grabPlanProgress where cellID is not null and endTime is null')
        resultValues = cursor.fetchall()
        for resultValue in resultValues:
            run_cell_planProgress(conn,resultValue)
        cursor.execute('select a.ID,b.url,a.region,a.totalCellPage,a.validCellPage,a.getCellPage,a.startTime,b.shortname \
        from grabPlanProgress a, city b \
        where a.region is not null and a.endTime is null and a.city = b.shortname order by ID')
        resultValues = cursor.fetchall()
        for resultValue in resultValues:
            run_region_planProgress(conn,resultValue)

    except Exception as e:
        print (e)
    cursor.close()


def run_region_planProgress(conn,regionInfo):
    progressID = regionInfo[0]
    url_prefix = regionInfo[1]
    regionname = regionInfo[2]
    totalCellPage = regionInfo[3]
    validCellPage = regionInfo[4]
    getCellPage = regionInfo[5]
    startTime = regionInfo[6]
    city = regionInfo[7]
    print (progressID)

    cursor = conn.cursor()
    if startTime is None:
        cursor.execute('update grabPlanProgress set startTIme=now() where ID=%s',progressID)

    if totalCellPage is None:
        url = url_prefix + "/xiaoqu/" + quote(regionname) + "/"
        print('get region url:' + url)
        soup = None
        while soup is None and len(proxys) > 0:
            soup = readurl_by_proxy(url)

        cell_number = int(soup.find('h2', {'class': 'total fl'}).span.get_text())
        if cell_number == 0:
            print('regionname :' + regionname + ' has 0 cell')

        page_info = soup.find('div', {'class': 'page-box house-lst-page-box'})
        if page_info is None:
            print('regionname :' + regionname + ' has no page-box house-lst-page-box item')
            return

        page_info_str = page_info.get('page-data').split(',')[0].split(':')[1]  # '{"totalPage":5,"curPage":1}'
        total_pages = int(page_info_str)
        print('run_region_planProgress:total_pages:' + page_info_str)

        cursor.execute('update grabPlanProgress set totalCellPage=%s where ID=%s',(total_pages,progressID))
    else:
        total_pages = totalCellPage

    if getCellPage is None:
        page = -1
    else:
        page = getCellPage - 1

    if validCellPage is None or page < validCellPage - 1:

        endSign = 0
        # for page in range(total_pages):
        while page < total_pages - 1:
            page = page + 1
            if page > 0:
                url_page = url_prefix + "/xiaoqu/" + quote(regionname) + "/pg%d/" % (page + 1)
                print('get region url:' + url_page)
                soup = None
                while soup is None and len(proxys) > 0:
                    soup = readurl_by_proxy(url_page)

            nameList = soup.findAll("li", {"class": "clear xiaoquListItem"})
            i = 0
            # -- get every house information and insert/update the mysql db
            for name in nameList:  # per house loop
                i = i + 1
                info_dict = {}

                celltitle = name.find("div", {"class": "title"})  # html
                info_dict.update({u'name': celltitle.a.get_text()})  # atrribute get
                info_dict.update({u'cellID': celltitle.a.get('href').split('/')[-2]})

                cellregion = name.find("a", {"class": "district"})
                info_dict.update({u'region': cellregion.get_text()})

                cellzone = name.find("a", {"class": "bizcircle"})
                info_dict.update({u'zone': cellzone.get_text()})

                cellevenprice = name.find("div", {"class": "totalPrice"})
                info_dict.update({u'evenSalePrice': cellevenprice.span.get_text()})

                cellonsale = name.find("div", {"class": "xiaoquListItemSellCount"})
                info_dict.update(({u'onSaleNumber': cellonsale.a.get_text()[:cellonsale.a.get_text().find('套')]}))

                onSaleNumber = cellonsale.a.get_text()[:cellonsale.a.get_text().find('套')]

                info_dict.update({u'obtainDate': datetime.now()})

                info_dict.update({u'progressID': progressID})

                info_dict.update({u'city': city})

                if onSaleNumber == '0' and cellevenprice.span.get_text() == '暂无均价':
                    endSign = 1
                    print('end of cell')
                    print('page is:' + str(page+1))
                    break

                # cellinfo insert into mysql
                cellinfo_insert_mysql(conn, info_dict)

                cursor.execute('update grabPlanProgress set getCellPage=%s where ID=%s',((page+1),progressID))

            if endSign == 1:
                cursor.execute('update grabPlanProgress set validCellPage=%s where ID=%s', ((page+1), progressID))
                break

    cursor.execute('select a.cellID,b.url from cellInfo a, city b where a.progressID=%s and a.onSaleNumber>0 \
    and a.getDetailSign=0 and a.city = b.shortname',progressID)
    resultValues = cursor.fetchall()
    for resultValue in resultValues:
        spider_house(conn,resultValue[0],progressID,resultValue[1])
        cursor.execute('update cellInfo set getDetailSign=1 where progressID=%s and cellID=%s',(progressID,resultValue[0]))
    cursor.execute('update grabPlanProgress set endTime=now() where ID=%s',progressID)
    cursor.close()


def run_cell_planProgress(conn,progressID):
    cursor = conn.cursor()
    try:
        cursor.execute('update grabPlanProgress set startTime=now() where ID=%s',progressID)
        cursor.execute('select a.ID, b.url, a.cellID from grabPlanProgress a, city b where a.ID=%s and a.city=b.shortname' ,progressID)
        resultValue = cursor.fetchone()
        progressID = resultValue[0]
        url_prefix = resultValue[1]
        cellID = resultValue[2]
        spider_cell(conn, cellID, progressID, url_prefix)
        spider_house(conn, cellID, progressID, url_prefix)
        cursor.execute('update grabPlanProgress set endTime=now() where ID=%s',progressID)
    except Exception as e:
        print (e)
    cursor.close()


def spider_cell(conn,cellID, progressID, url_prefix):
    url = url_prefix + "/xiaoqu/c" + cellID + "/"
    print('get cell url:' + url)
    soup = None
    while soup is None and len(proxys)>0:
        soup = readurl_by_proxy(url)
    cell_number = int(soup.find('h2', {'class': 'total fl'}).span.get_text())
    print('cellID :' + cellID + ' has ' + str(cell_number) + ' cell to found')
    if cell_number == 0:
        return soup

    nameList = soup.findAll("li", {"class": "clear xiaoquListItem"})

    for name in nameList:
        info_dict = {}

        celltitle = name.find("div", {"class": "title"})
        info_dict.update({u'name': celltitle.a.get_text()})
        info_dict.update({u'cellID': celltitle.a.get('href').split('/')[-2]})

        cellregion = name.find("a", {"class": "district"})
        info_dict.update({u'region': cellregion.get_text()})

        cellzone = name.find("a", {"class": "bizcircle"})
        info_dict.update({u'zone': cellzone.get_text()})

        cellevenprice = name.find("div", {"class": "totalPrice"})
        info_dict.update({u'evenSalePrice': cellevenprice.span.get_text()})

        cellonsale = name.find("div", {"class": "xiaoquListItemSellCount"})
        info_dict.update(({u'onSaleNumber': cellonsale.a.get_text()[:cellonsale.a.get_text().find('套')]}))

        info_dict.update({u'obtainDate': datetime.now()})

        info_dict.update({u'progressID': progressID})

        cellinfo_insert_mysql(conn, info_dict)

    return soup

def spider_house(conn,cellID,progressID,url_prefix):
    url = url_prefix + "/ershoufang/c" + cellID + "/"
    print('get ershoufang of pointed cell url:' + url)

    soup = None
    while soup is None and len(proxys) > 0 :
        soup = readurl_by_proxy(url)

#==============================================================================

    cell_number = int(soup.find('h2', {'class': 'total fl'}).span.get_text())
    if cell_number == 0:
        print ('cellID :' + cellID + ' has 0 onSale house')

    page_info= soup.find('div',{'class':'page-box house-lst-page-box'})
    if page_info is None:
        print ('cellID :' + cellID + ' has no page-box house-lst-page-box item')
        return

    page_info_str = page_info.get('page-data').split(',')[0]  #'{"totalPage":5,"curPage":1}'
    total_pages= int(page_info_str[-1])
    print ('cellID is :' + cellID + ', spider_house:total_pages:'+page_info_str[-1])
    if total_pages == 0:
        total_pages = total_pages + 1
        print (page_info)
        print ('total_pages is 0')
        print (page_info_str[-1])
        print (int(page_info_str[-1]))
    if page_info_str[-1] is None:
        print (soup)
        print (page_info_str)

    page = -1
    #for page in range(total_pages):
    while page < total_pages - 1:
        page = page + 1
        if page>0:
            url_page= url_prefix + "/ershoufang/pg%dc%s/" % (page+1,cellID)
            print ('url is:' + url_page)
            soup = None
            while soup is None  and len(proxys) > 0 :
                soup = readurl_by_proxy(url_page)

        nameList = soup.findAll("li", {"class":"clear"})
        i = 0
        #-- get every house information and insert/update the mysql db
        print ('cellID:' + cellID + ', page:' + str(page+1))
        for name in nameList:   # per house loop
            i = i + 1
            info_dict = {}
            housetitle = name.find("div",{"class":"title"})  #html
            info_dict.update({u'link':housetitle.a.get('href')})   #atrribute get

            houseaddr = name.find("div",{"class":"houseInfo"})
            info = houseaddr.get_text().split('|')
            info_dict.update({u'type':info[1]})
            info_dict.update({u'square':info[2][0:info[2].find('平米')]})
            info_dict.update({u'direction':info[3]})

            housefloor = name.find("div",{"class":"flood"})
            floor_all = housefloor.div.get_text().split('-')[0].strip().split(' ')
            info_dict.update({u'floor':floor_all[0]})
            info_dict.update({u'years':floor_all[-1]})

            followInfo = name.find("div",{"class":"followInfo"}).get_text().split('/')
            info_dict.update({u'focusNumber': followInfo[0][:followInfo[0].find('人')]})
            info_dict.update({u'visitNumber': followInfo[1][2:followInfo[1].find('次')]})
            info_dict.update({u'roughDate': followInfo[2]})

            totalPrice = name.find("div",{"class":"totalPrice"})
            info_dict.update({u'totalPrice':totalPrice.span.get_text()})

            unitPrice = name.find("div",{"class":"unitPrice"})
            info_dict.update({u'unitPrice':unitPrice.get('data-price')})
            info_dict.update({u'houseID':unitPrice.get('data-hid')})
            info_dict.update({u'cellID': unitPrice.get('data-rid')})

            info_dict.update({u'obtainDate':datetime.now()})

            info_dict.update({u'progressID':progressID})

            # houseinfo insert into mysql
            houseinfo_insert_mysql(conn,info_dict)

    cellinfo_getupdate_mysql(conn,cellID)



def readurl_by_proxy(url):
    try:
        tet = proxys[random.randint(0, len(proxys) - 1)]
        proxy_support = urllib.request.ProxyHandler(tet)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        req = Request(url, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urlopen(req, timeout=10).read()
        if source_code.find(b'\xe6\x82\xa8\xe6\x89\x80\xe5\x9c\xa8\xe7\x9a\x84IP') != -1:
            proxys.remove(tet)
            print('proxys remove by IP traffic, new length is:' + str(len(proxys)))
            return None
        soup = BeautifulSoup(source_code, 'lxml')

        cell_number = int(soup.find('h2', {'class': 'total fl'}).span.get_text()) # 使用一些IP代理，访问url可能会有返回，却是别的页面。

        if cell_number is None:
            proxys.remove(tet)
            print('total page is None')
            print('proxys new length is:' + str(len(proxys)))
            return None
    except Exception as e:
        proxys.remove(tet)
        print('proxys remove by exception:')
        print (e)
        print ('proxys new length is:' + str(len(proxys)))
        return None

    return soup


def proxy_init():
    f = open("ProxyIP.txt")
    lines = f.readlines()
    for i in range(0,len(lines)):
        proxy_host = lines[i].strip("\n").split("\t")
        proxy_temp = {"http":proxy_host[0]}
        proxys.append(proxy_temp)

def spider_proxyip():
    try:
        for i in range(1,4):
            url='http://www.xicidaili.com/nt/' + str(i)
            req = Request(url,headers=hds[random.randint(0, len(hds) - 1)])
            source_code = urlopen(req).read()
            soup = BeautifulSoup(source_code,'lxml')
            ips = soup.findAll('tr')

            for x in range(1,len(ips)):
                ip = ips[x]
                tds = ip.findAll("td")
                proxy_host = "http://" + tds[1].contents[0]+":"+tds[2].contents[0]
                proxy_temp = {"http":proxy_host}
                proxys_src.append(proxy_temp)
    except Exception as e:
        print ("spider_proxyip exception:")
        print (e)

def test_proxyip_thread(i):
    socket.setdefaulttimeout(5)
    url = "http://bj.lianjia.com"
    try:
        proxy_support = urllib.request.ProxyHandler(proxys_src[i])
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        res = urllib.request.Request(url,headers=hds[random.randint(0, len(hds) - 1)])
        source_cod = urllib.request.urlopen(res,timeout=10).read()
        if source_cod.find(b'\xe6\x82\xa8\xe6\x89\x80\xe5\x9c\xa8\xe7\x9a\x84IP') == -1:
            proxys.append(proxys_src[i])
    except Exception as e:
        return
       # print(e)

def test_proxyip():
    print ("proxys before:"+str(len(proxys_src)))
    threads = []
    try:
        for i in range(len(proxys_src)):
            thread = threading.Thread(target=test_proxyip_thread, args=[i])
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
    except Exception as e:
        print (e)
    print ("proxys after:" + str(len(proxys)))



def archive_history(conn):
    now = datetime.now()
    hour = now.hour
    minute = now.minute
    if hour == 23 and (minute >=0 and minute <= 5):
        cursor = conn.cursor()
        cursor.execute('insert into cellInfo_H select * from cellInfo')
        cursor.execute('insert into onSaleHouseInfo_H select * from onSaleHouseInfo')
        cursor.execute('truncate table cellInfo')
        cursor.execute('truncate table onSaleHouseInfo')
        cursor.close()

def grabPlan_append(conn):
    now = datetime.now()
    hour = now.hour
    minute = now.minute
    if hour == 9 and (minute >= 0 and minute <= 5):
        cursor = conn.cursor()
        cursor.execute("select count(*) from grabPlan where type=1 and city='bj' and \
            ( onProgress=0 or date_format(createTime,'%Y-%m-%d')>= date_format(now(),'%Y-%m-%d') ) ")
        resultValue = cursor.fetchone()
        if resultValue[0] != 1:
            cursor.execute("insert into grabPlan (type,city,onProgress,createTime) values (1,'bj',0,now())")
        cursor.close()

def prepare_proxy(conn):
    cursor = conn.cursor()
    cursor.execute('select count(*) from grabPlanProgress where endTime is null')
    resultValue = cursor.fetchone()
    if resultValue[0] > 0:
        spider_proxyip()
        test_proxyip()


if __name__=="__main__":

    dbflag = 'local'            # local,  remote

    conn = database_init(dbflag)
    try:
        while True:
            grabPlan_append(conn)

            convert_plan_to_progress(conn)

            prepare_proxy(conn)
            
            run_planProgress(conn)

            archive_history(conn)

            print ('begin sleep 10 minutes...')

            time.sleep(600)
    except Exception as e:
        conn.close()
        print (e)
    print ('process out')
