#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Library for retrieving data from web
import sys
import urllib, urllib2, httplib
import requests
import datetime, time
from pandas import DataFrame
from lxml import etree
import tushare as ts
import socket
import logging
# historical url
_HISTORICAL_HEXUN_URL  = 'http://data.funds.hexun.com/outxml/detail/openfundnetvalue.aspx?'
_HISTORICAL_GOOGLE_URL = 'http://www.google.com/finance/historical?'
_HISTORICAL_163_URL    = 'http://quotes.money.163.com/fund/'

def NotExchFund(ticker):
    return ticker.startswith('16')
    
def get_fund_quote_with_value(ticker="150033", useLogging=False):
    ''' a unified interface to get real time quote with value '''
    df = None
    try:
        if NotExchFund(ticker):
            df = get_fund_quote_est_em(ticker)
            df['b1_p']       = df['price']
            df['a1_p']       = df['price']
            df['b1_v']       = 100000
            df['a1_v']       = 100000
            df['value']      = df['price']
            df['value_date'] = df['date']
        else:
            df = get_fund_quote_ts(ticker)
    except Exception as e:
        if useLogging:
            logging.warning("get quote failed with error %s ticker %s"%(str(e), ticker))
        else:
            print "get quote failed with error %s ticker %s"%(str(e), ticker)
        # if price not avaialbe return none
        return None
        
    try:
        if not NotExchFund(ticker):
            valdf = get_fund_quote_est_em(ticker)
            df['value']      = valdf['price']
            df['value_date'] = valdf['date']
    except Exception as e:
        if useLogging:
            logging.warning("get value failed with error %s ticker %s"%(str(e), ticker))
        else:
            print "get value failed with error %s ticker %s"%(str(e), ticker)
        df['value']      = -999.0 # suggest failure in price
        df['value_date'] = df['date']
    return df
    
def wrapper(retry=5, timeout=3.0, interval=0.005):
    def decorator(func):
        def call(*args, **kwargs):
            previous_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(timeout)
            count = 0
            df = None
            while count < retry:
                try:
                    df = func(*args, **kwargs)
                except Exception as e:
                    #print "call %s failed with error %s"%(func.__name__, str(e))
                    logging.warning("call %s failed with error %s"%(func.__name__, str(e)))
                if not (df is None or df.empty):
                    break
                time.sleep(interval)
                count += 1
            socket.setdefaulttimeout(previous_timeout)
            return df
        return call
    return decorator
    
# retrieve fund quote from hexun
@wrapper()
def get_fund_quote_hx(ticker='150019'):
    url      = 'quote.stock.hexun.com'
    path     = '/stockdata/fund_quote.aspx?stocklist=' + ticker
    conn     = httplib.HTTPConnection(url)
    conn.request("GET", path)
    resp     = conn.getresponse()
    #print ticker, resp.status, resp.reason
    data     = resp.read()
    dataList = data.split('[[')[1].split(']]')[0].split(',')
    print dataList
    assert ticker == dataList[0][1:-1], "input ticker %s inconsistent to output ticker %s" % (ticker, dataList[0][1:-1])
    outdf = DataFrame({'ticker': ticker, 'price': float(dataList[2]), 'pre_close': float(dataList[4]), 'open': float(dataList[5]), 'high': float(dataList[6]), 'low': float(dataList[7])},index=[datetime.date.today()])
    return outdf

#retrieve real time quotes from tushare
@wrapper()
def get_fund_quote_ts(ticker='150033'):
    '''
name               金融地A     open              1.093     pre_close         1.080     price             1.083     high              1.095
low               1.081         bid               1.083    ask               1.093      volume           109100     amount       119215.600
b1_v                148         b1_p              1.083     b2_v                100     b2_p              1.081     b3_v                 35
b3_p              1.078         b4_v                100     b4_p              1.077     b5_v                200     b5_p              1.075
a1_v                460         a1_p              1.093     a2_v                669     a2_p              1.094     a3_v               1303
a3_p              1.095         a4_v                250     a4_p              1.099     a5_v            a5_p              0.000
date         2016-08-25         time           15:05:03     code             150281
    '''
    df = ts.get_realtime_quotes(ticker)
    if df is not None:
        strCols = df[["name", "date", "time", "code"]]
        strCols = strCols.assign(date=lambda x: datetime.datetime.strptime(x.date.iloc[0]+" "+x.time.iloc[0], "%Y-%m-%d %H:%M:%S"))
        strCols = strCols.drop("time", axis=1)
        df = df.drop(["name", "date", "time", "code"], axis=1)
        df = df.replace(to_replace="", value="nan")
        df = df.astype(float)
        df = df.join(strCols)
        df = df.assign(data_src="ts")
    return df

# retrieve fund est value from eastmoney
@wrapper()
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
    df = DataFrame(data = [[ticker, esttime, estval, "est"]], index = [0],
                      columns = ['code', 'date', 'price', 'data_src'])
    return df

def get_fund_quote(ticker="150033"):
    ''' a unified interface to get real time quote '''
    try:
        df = get_fund_quote_ts(ticker)
    except Exception as e:
        print "get tushare quote failed with error %s"%(str(e))
        df = None
    if df is None or df.empty:
        try:
            df = get_fund_quote_est_em(ticker)
        except Exception as e:
            print "get eastmoney quote failed with error %s" % (str(e))
            df = None
    return df

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
    
    header = ['Date', 'Close', 'PriceChange', 'Volume', 'Amount', 'Turnover', 'Discount']
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