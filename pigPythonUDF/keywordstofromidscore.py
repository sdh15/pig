#!/usr/bin/python
#coding=utf8

import types
import math
import sys
import urllib2
import urllib
from com.xhaus.jyson import JysonCodec as json

reload(sys)
sys.setdefaultencoding('UTF-8')
fromidscoreDict={}
@outputSchema("y:bag{t:tuple(fromid:chararray,score:double)}")
def keywords2fromidscore(bag):
	keywordDict=generateKeywordDict(bag)
	for (keyword,count) in keywordDict.items():
		add2FromidscoreDict(keyword,count)
	return fromidscoreDict.items()
def add2FromidscoreDict(keyword,count):
	fromidScoreTupList=keyword2fromidScoreTupList(keyword)
	for fromidScoreTup in fromidScoreTupList:
		fromid=fromidScoreTup[0]
		if fromid in fromidscoreDict:
			fromidscoreDict[fromid]=fromidscoreDict[fromid]+count*fromidScoreTup[1]
		else:
			fromidscoreDict[fromid]=count*fromidScoreTup[1]
def generateKeywordDict(bag):
	dict = {}
	for keyword in bag:
 		if keyword not in dict:
			dict[keyword]=1
 		else:
			dict[keyword]=dict[keyword]+1
	return dict
def keyword2fromidScoreTupList(keyword):
        fromidScoreTupList=[]
	encodedKeyWord=None
	try:
		encodedKeyWord=urllib.quote(str(keyword))
	except  Exception,e:
		print "error happened when quote keyword: "+keyword,e
	if encodedKeyWord is None:
		return fromidScoreTupList
	req=urllib2.Request('http://lc1.haproxy.yidian.com:8902/service/assistant?word='+urllib.quote(str(keyword)))
        try:
                resonse=urllib2.urlopen(req)
	except urllib2.URLError, e:
		if hasattr(e, 'reason'):
			print keyword
			print 'We failed to reach a server.'
			print 'Reason: ', e.reason
		elif hasattr(e, 'code'):
			print 'The server couldn\'t fulfill the request.'
        		print 'Error code: ', e.code
	else:
                res=resonse.read()
                rootList=json.loads(res)
                channels=rootList['channels']
                for channel in channels:
                        fromidScoreTup=(channel['id'],1.0)
                        fromidScoreTupList.append(fromidScoreTup)
                        #print fromidScoreTup
                        if len(fromidScoreTupList) == 2:
                                break
        return fromidScoreTupList
#if __name__ == "__main__":
#	keywords2fromidscore(['定酒店哪个网站最便宜','预定酒店用什么软件好','定酒店哪个网站最便宜','预定酒店用什么软件好'])
#	for (fromid,score) in fromidscoreDict.items():
#		print fromid,score
