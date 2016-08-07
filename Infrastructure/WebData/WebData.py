#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Library for retrieving data from web
import sys
import urllib, urllib2
import requests
import datetime, time
from pandas import DataFrame
from lxml import etree

# historical url
_HISTORICAL_HEXUN_URL  = 'http://data.funds.hexun.com/outxml/detail/openfundnetvalue.aspx?'
_HISTORICAL_GOOGLE_URL = 'http://www.google.com/finance/historical?'
_HISTORICAL_163_URL    = 'http://quotes.money.163.com/fund/'

# retrieve fund est value from eastmoney
def get_fund_quote_est_em(ticker='161121'):
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

# retrieve fund historical value from netease
def get_fund_value_data_163(ticker='150008', start=datetime.date(2016,1,1), end=datetime.date(2016,6,1), asc=False):
    url  = _HISTORICAL_163_URL
    url += "jzzs_" + ticker + ".html?"
    url += "start=" + start.strftime("%Y-%m-%d")
    url += "&end=" + end.strftime("%Y-%m-%d")
    if asc:
        url += "&sort=TDATE&order=asc"
    else:
        url += "&sort=TDATE&order=desc"
    print url
    
    web        = urllib.urlopen(url)
    s          = web.read()
    html       = etree.HTML(s)
    num_nodes  = html.xpath('//div[@class="mod_pages"]/a')
    num_list   = [node.text for node in num_nodes]
    num_page   = 1
    if len(num_list) > 0:
        assert num_list[-1] == u'\u4e0b\u4e00\u9875', "last page should be next page"
        num_page = int(num_list[-2])
    
    def fmt163(tr):
        tdlist = [td for td in tr.xpath('td')]
        pxdate = datetime.datetime(*(time.strptime(tdlist[0].text.strip(), "%Y-%m-%d")[0:6])),
        px     = float(tdlist[1].text.strip())
        pxaccu = float(tdlist[2].text.strip())
        pxchg  = float(tdlist[3].xpath('span')[0].text.strip('%').replace('--','0'))/100.
        return [pxdate, px, pxaccu, pxchg]
                 
    td_content = []
    for i in xrange(num_page):
        suburl  = url.replace(ticker, ticker+'_'+str(i))
        print "processing " + suburl
        web     = urllib.urlopen(suburl)
        s       = web.read()
        html    = etree.HTML(s)
        nodes   = html.xpath('//div[@id="fn_fund_value_trend"]/table/tbody/tr')
        tdtable = [fmt163(tr) for tr in nodes]
        td_content += tdtable
    
    header = ['Date', 'UnitValue', 'AccuValue', 'ValueChange']
    df     = DataFrame(data=td_content, columns=header)
    df     = df.set_index(['Date'])
    return df
    
# retrieve fund historical price from netease
def get_fund_price_data_163(ticker='150008', start=datetime.date(2016,1,1), end=datetime.date(2016,6,1), asc=False):
    url  = _HISTORICAL_163_URL
    url += "zyjl_" + ticker + ".html?"
    url += "start=" + start.strftime("%Y-%m-%d")
    url += "&end=" + end.strftime("%Y-%m-%d")
    if asc:
        url += "&sort=TDATE&order=asc"
    else:
        url += "&sort=TDATE&order=desc"
    print url
    
    web        = urllib.urlopen(url)
    s          = web.read()
    html       = etree.HTML(s)
    num_nodes  = html.xpath('//div[@class="mod_pages"]/a')
    num_list   = [node.text for node in num_nodes]
    num_page   = 1
    if len(num_list) > 0:
        assert num_list[-1] == u'\u4e0b\u4e00\u9875', "last page should be next page"
        num_page = int(num_list[-2])
    
    def fmt163(tr):
        tdlist    = [td for td in tr.xpath('td')]
        # Date
        pxdate    = datetime.datetime(*(time.strptime(tdlist[0].text.strip(), "%Y-%m-%d")[0:6])),
        # UnitPrice
        px        = float(tdlist[1].text.strip())
        # PriceChange
        pxchg     = float(tdlist[2].xpath('span')[0].text.strip('%').replace('--','0').replace(',',''))/100.
        # Volumn
        volstr    = tdlist[3].text.strip().replace('--','0').replace(',','')
        check10k  = u'\u4e07' in volstr
        check100m = u'\u4ebf' in volstr
        volstp    = volstr.replace(u'\u4e07', '').replace(u'\u4ebf', '')
        if check10k:
            volume = 10000.*float(volstp)
        elif check100m:
            volume = 100000000.*float(volstp)
        else:
            volume = float(volstr)
        # Amount
        amtstr    = tdlist[4].text.strip().replace('--','0').replace(',','')
        check10k  = u'\u4e07' in amtstr
        check100m = u'\u4ebf' in amtstr
        amtstp = amtstr.replace(u'\u4e07', '').replace(u'\u4ebf', '')
        if check10k:
            amount = 10000.*float(amtstp)
        elif check100m:
            amount = 100000000.*float(amtstp)
        else:
            amount = float(amtstp)
        # Percentage change
        pctchg = float(tdlist[5].text.strip('%').replace('--','0').replace(',',''))/100.
        # Over/Under price
        ovund  = float(tdlist[6].xpath('span')[0].text.strip('%').replace('--','0').replace(',',''))/100.
        return [pxdate, px, pxchg, volume, amount, pctchg, ovund]
                 
    td_content = []
    for i in xrange(num_page):
        suburl  = url.replace(ticker, ticker+'_'+str(i))
        print "processing " + suburl
        web     = urllib.urlopen(suburl)
        s       = web.read()
        html    = etree.HTML(s)
        nodes   = html.xpath('//div[@id="fn_fund_value_trend"]/table/tbody/tr')
        tdtable = [fmt163(tr) for tr in nodes]
        td_content += tdtable
    
    header = ['Date', 'Close', 'PriceChange', 'Volumn', 'Amount', 'TurnOver', 'Discount']
    df     = DataFrame(data=td_content, columns=header)
    df     = df.set_index(['Date'])
    return df
    
# retrieve fund historical net value from hexun
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