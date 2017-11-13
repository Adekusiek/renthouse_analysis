from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import pandas as pd
from pandas import Series, DataFrame
import time

#URL chiyodaku
url = "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13101&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1&pn=2"
urllink = "dummy"

#get data
while urllink != None:
    result = requests.get(url)
    soup = BeautifulSoup(result.content, "html.parser")
#js-leftColumnForm > div.pagination_set > div.pagination.pagination_set-nav > p:nth-child(3)

# get data

# go to next page

#js-bukkenList

    urllink = soup.select(".pagination_set > .pagination-parts")
    print(urllink)
    url = urljoin(url, urllink)

    print("url:", url)
    time.sleep(1)

print("No further page found")
