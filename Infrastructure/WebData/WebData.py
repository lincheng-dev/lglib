#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Library for retrieving data from web
import sys
import urllib, urllib2
import requests
import datetime, time
import signal
from pandas import DataFrame
from lxml import etree

# for web scraping with Qt
import PySide
from PySide.QtCore import QUrl
from PySide.QtGui import QApplication
from PySide.QtWebKit import QWebPage

# historical url
_HISTORICAL_HEXUN_URL  = 'http://data.funds.hexun.com/outxml/detail/openfundnetvalue.aspx?'
_HISTORICAL_GOOGLE_URL = 'http://www.google.com/finance/historical?'

# retrieve fund est value from eastmoney
def get_fund_value_est_em(ticker='161121'):
    # need to validate fund
    # looks quite slow
    url      = 'http://fund.eastmoney.com/'+ticker+'.html'
    web      = urllib.urlopen(url)
    s        = web.read()
    html     = etree.HTML(s)
    tr_nodes = html.xpath('//div[@class="fundInfoItem"]/div[@class="dataOfFund"]')
    estvstr  = tr_nodes[0].xpath('.//span[@id="gz_gsz"]')
    estval   = float(estvstr[0].text)
    esttmstr = tr_nodes[0].xpath('.//span[@id="gz_gztime"]')
    esttime  = datetime.datetime(*(time.strptime(esttmstr[0].text, "(%y-%m-%d %H:%M)")[0:6]))
    return (esttime, estval)

class WebParser(QWebPage):
    # get html after execution
    # need more tests here
    # the JS code is as below, can we directly execute?
    # <script type="text/javascript">
    #         LoadFundSelect("jjlist", "jjjz");
    #         ChkSelectItem("jjlist", "161121");
    #         var params = { code: strbzdm, pindex: 1, pernum: 20, startDate:"",endDate:"" };
    #         LoadLsjz(params);
    # </script>
    def __init__(self, url, filename):
        self._file = filename
        self.app = QApplication.instance()
        QWebPage.__init__(self)
        self.loadFinished.connect(self._loadFinished)
        self.mainFrame().load(QUrl(url))
        self.app.exec_()
            
    def _loadFinished(self, result):
        self.frame = self.mainFrame()
        filehandle = open(self._file, 'w')
        filehandle.write(self.mainFrame().toHtml().encode('UTF-8'))
        filehandle.close()
        self.app.quit() 

# retrieve fund historical data from hexun
def get_fund_value_data_hx(ticker='161121', start=datetime.date(2016,1,1), end=datetime.date(2016,6,1)):
    url  = _HISTORICAL_HEXUN_URL
    url += "fundcode=" + ticker
    url += "&startdate=" + start.strftime("%Y-%m-%d")
    url += "&enddate=" + end.strftime("%Y-%m-%d")
    print url
      
    tree     = etree.parse(url)
    nodes    = tree.xpath('//Data')
    datalist = []
    header   = ['Date', 'UnitValue', 'AccuValue', 'NewValue']
    for node in nodes:
        datalist.append([child.text for child in node])
    df       = DataFrame(data=datalist, columns=header)
    df       = df.set_index(['Date'])
    df.index = df.index.map(lambda x: datetime.datetime(*(time.strptime(x, "%Y-%m-%d")[0:6])))
    for col in df:
        df[col] = df[col].map(lambda x: float(x.replace(',','')))
    return df    
        
# retrieve data from google
def get_data_google(ticker='MUTF_CN:150048', start=datetime.date(2016,1,1), end=datetime.date(2016,6,1)):
    # first prepare Url
    numDays = (end - start).days
    url  = _HISTORICAL_GOOGLE_URL
    url += "q=" + ticker
    url += "&startdate=" + start.strftime("%b+%d, +%Y")
    url += "&enddate=" + end.strftime("%b+%d, +%Y")
    url += "&start=0"
    url += "&num=" + str(numDays)
    print url
    
    web        = urllib.urlopen(url)
    s          = web.read()
    html       = etree.HTML(s)
    tr_nodes   = html.xpath('//table[@class="gf-table historical_price"]/tr')
    header     = [i.text.strip() for i in tr_nodes[0].xpath("th")]
    td_content = [[td.text.strip() for td in tr.xpath('td')] for tr in tr_nodes[1:]]
    df         = DataFrame(data=td_content, columns=header)
    df         = df.set_index(['Date'])
    df.index   = df.index.map(lambda x: datetime.datetime(*(time.strptime(x, "%b %d, %Y")[0:6])))
    for col in df:
        df[col] = df[col].map(lambda x: float(x.replace(',','')))
    return df

# SSE A-share list
def GetSSEShareCode():
    
    url   = "http://www.sse.com.cn/js/common/ssesuggestdata.js"
    req   = urllib2.Request(url)
    sList = []
    
    try:
        data = urllib2.urlopen(req)
        for item in data:
            if "push" in item:
                sList.append(item.split("\"")[1])
    except urllib2.URLError, e:
        print e.reason
        
    return sList            

# SZSE A-share list
def GetSZSEShareCode(updateSource = False):
    
    url   = "http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1110&tab1PAGENUM=1&ENCODE=1&TABKEY=tab1"
    fout  = "K:\Temp\SZSETemp.html"
    sList = []
    
    if updateSource:
        print "Retrieving stock list from SZSE"
        with open(fout, 'wb') as handle:
            resp = requests.get(url, stream=True)
            if not resp.ok:
                raise urllib2.URLError("SZSE unaccessible")
        
            count = 0
            for block in resp.iter_content(1024):
                handle.write(block)
                count += 1
                if count % 100 == 0:
                    print "%d KB finished" % count
    
    from lxml.html import parse
    page = parse(fout)
    rows = page.xpath("body/table")[0].findall("tr")
    data = list()
    for row in rows:
        data.append([c.text for c in row.getchildren()])
        sList.append(data[-1][0])
        
    return sList[1:]
    
if __name__ == "__main__":
    SSEList  = GetSSEShareCode()
    SZSEList = GetSZSEShareCode()
    print len(SSEList), len(SZSEList)