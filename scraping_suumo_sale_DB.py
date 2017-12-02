from bs4 import BeautifulSoup
import urllib.request as req
from urllib.parse import urljoin
import pandas as pd
import time
from datetime import datetime
import pymysql.cursors

#URL chiyodaku
town_urls = [["chiyoda", "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13101&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["chuo",    "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13102&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["minato",  "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13103&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["shinjuku","http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13104&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["bunkyo",  "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13105&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["shibuya", "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13113&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["taito",   "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13106&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["sumida",  "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13107&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["koto",    "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13108&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["shinagawa","http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13109&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["meguro",  "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13110&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["ota",     "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13111&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["setagaya","http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13112&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["nakano",  "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13114&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["suginami","http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13115&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["toshima", "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13116&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["kita",    "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13117&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["arakawa", "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13118&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["itabashi","http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13119&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["nerima",  "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13120&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["adachi",  "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13121&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["katushika", "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13122&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ["edogawa",   "http://suumo.jp/jj/bukken/ichiran/JJ010FJ001/?ar=030&bs=011&ta=13&jspIdFlg=patternShikugun&sc=13123&kb=1&kt=9999999&mb=0&mt=9999999&ekTjCd=&ekTjNm=&tj=0&cnb=0&cn=9999999"],
            ]

def scrape_from_suumo(url_input):
    state_name = url_input[0] # set state name
    url = url_input[1]        # set url for each initial page
    urllink = "dummy"

    #get data
    while urllink != None:
        urllink = None
        sub_urllinks = []
        #try until no error is returned
        res = None
        while res is None:
            try:
                res = req.urlopen(url)
            except:
                pass

        soup = BeautifulSoup(res, "html.parser")

        # set url path to each appart page
        content_links = soup.select("#js-bukkenList .property_unit-header h2")
        for content_link in content_links:
            sub_urllinks.append(urljoin(url, content_link.a.attrs["href"]))

        # read attributes on each appartment page
        for sub_urllink in sub_urllinks:
            # check if the data exists in database
            c = db.cursor()
            sql_get = "select * from sale_appartments where url like '%s';" % sub_urllink
            c.execute(sql_get)
            connect = c.fetchall()
            c.close()
            if len(connect) >= 1:
                print(connect)
                continue

            #try until no error is returned
            subres = None
            while subres is None:
                try:
                    subres = req.urlopen(sub_urllink)
                except:
                    pass

            sub_soup = BeautifulSoup(subres, "html.parser")

            contents = sub_soup.select(".mt20 table[summary='表'] tr")

            if contents[0].td.string is None: continue
            name = contents[0].td.string.replace('\t', '').replace('\r', '').replace('\n', '')
            address = contents[5].select("td")[0].select("p")[0].string
            year = contents[4].select("td")[1].string.replace('\t', '').replace('\r', '').replace('\n', '')
            story = contents[4].select("td")[0].string.replace('\t', '').replace('\r', '').replace('\n', '')
            floor_plan = contents[1].select("td")[1].p.string.replace('\t', '').replace('\r', '').replace('\n', '')
            room_num = contents[2].select("td")[1].string.replace('\t', '').replace('\r', '').replace('\n', '')
            surface = contents[3].select("td")[0].find(text=True).replace('\t', '').replace('\r', '').replace('\n', '')
            other_surface = contents[3].select("td")[1].find(text=True).replace('\t', '').replace('\r', '').replace('\n', '')
            price = contents[1].select("td")[0].p.string.replace('\t', '').replace('\r', '').replace('\n', '')
            station1 = contents[5].select("td")[1].select("div")[0].string
            try:
                station2 = contents[5].select("td")[1].select("div")[1].string
            except:
                station2 = None

            try:
                station3 = contents[5].select("td")[1].select("div")[2].string
            except:
                station3 = None

            c = db.cursor()

            sql_create = "insert into sale_appartments(name, address, station1, station2, station3, year, story,  \
                                    room_num, floor_plan, surface, other_surface, price, url, created_at, updated_at) \
                        values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" \
                        %  (name, address, station1, station2, station3, year, story,  \
                            room_num, floor_plan, surface, other_surface, price, sub_urllink, \
                            datetime.now(), datetime.now())

            c.execute(sql_create)
            db.commit()
            c.close()

            time.sleep(1)

        # get link to next page
        rel_url_list = soup.select(".pagination_set  .pagination_set-nav > p")
        for rel_url in rel_url_list:
            if rel_url.a.string == "次へ":
                urllink = rel_url.a.attrs["href"]
        # get absolute path for next page
        url = urljoin(url, urllink)

        print("url:", url)

    print("No further page found")

db = pymysql.connect(
        host = 'localhost',
        user = 'root',
        password = '',
        db = 'suumo_monthly_rent_development',
        charset = 'utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

for town_url in town_urls:
    scrape_from_suumo(town_url)
    print(town_url[0], "is finished")

db.close()
