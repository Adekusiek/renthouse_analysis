from bs4 import BeautifulSoup
import urllib.request as req
from urllib.parse import urljoin
import pandas as pd
from pandas import Series, DataFrame
import time
from datetime import datetime
import pymysql.cursors

#URL chiyodaku
town_urls = [["chiyoda", "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13101&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["chuo",    "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13102&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["minato",  "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13103&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["shinjuku","http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13104&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["bunkyo",  "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13105&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["shibuya", "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13113&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["taito",   "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13106&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["sumida",  "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13107&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["koto",    "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13108&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["shinagawa","http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13109&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["meguro",  "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13110&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["ota",     "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13111&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["setagaya","http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13112&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["nakano",  "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13114&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["suginami","http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13115&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["toshima", "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13116&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["kita",    "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13117&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["arakawa", "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13118&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["itabashi","http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13119&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["nerima",  "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13120&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["adachi",  "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13121&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["katushika", "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13122&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ["edogawa",   "http://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc=13123&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&srch_navi=1"],
            ]

def scrape_from_suumo(url_input):
    state_name = url_input[0] # set state name
    url = url_input[1]        # set url for each initial page
    urllink = "dummy"

    #get data
    while urllink != None:
        urllink = None
        #try until no error
        res = None
        while res is None:
            try:
                res = req.urlopen(url)
            except:
                pass

        soup = BeautifulSoup(res, "html.parser")

        contents = soup.select("#js-bukkenList .cassetteitem")
        for content in contents:
            for detail_content in content.select(".cassetteitem_other tbody"):
                c = db.cursor()
                url_check = detail_content.select("td")[10].a.attrs["href"]
                sql_get = "select * from appartments where url like '%s';" % url_check
                c.execute(sql_get)
                connect = c.fetchall()
                c.close()
                # skip if the same link already exists in the database
                if len(connect) >= 1:
                    continue

                # in case parking space is recorded
                if not '階' in detail_content.select("td")[2].string:
                    continue
                detail_url = detail_content.select("td")[10].a.attrs["href"]
                floor = detail_content.select("td")[2].string
                rent = detail_content.select("td")[3].string
                admin_fee = detail_content.select("td")[4].string
                initial_cost = detail_content.select("td")[5].string
                floor_plan = detail_content.select("td")[6].string
                surface = detail_content.select("td")[7].find(text=True)
                name = content.select_one(".cassetteitem_content-title").string
                address = content.select_one(".cassetteitem_detail-col1").string
                age = content.select(".cassetteitem_detail-col3 div")[0].string
                story = content.select(".cassetteitem_detail-col3 div")[1].string
                station1 = content.select(".cassetteitem_detail-col2 div")[0].string
                try:
                    station2 = content.select(".cassetteitem_detail-col2 div")[1].string
                except:
                    station2 = None

                try:
                    station3 = content.select(".cassetteitem_detail-col2 div")[2].string
                except:
                    station3 = None

                c = db.cursor()

                sql_create = "insert into appartments(name, address, station1, station2, station3, age, story,  \
                                        floor, rent, admin_fee, initial_cost, floor_plan, surface, url, created_at, \
                                        updated_at) \
                            values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" \
                            %  (name, address, station1, station2, station3, age, story,  \
                                floor, rent, admin_fee, initial_cost, floor_plan, surface, detail_url, \
                                datetime.now(), datetime.now())

                c.execute(sql_create)
                db.commit()
                c.close()

        # get link to next page
        rel_url_list = soup.select(".pagination_set  .pagination_set-nav > p")
        for rel_url in rel_url_list:
            if rel_url.a.string == "次へ":
                urllink = rel_url.a.attrs["href"]
        # get absolute path for next page
        url = urljoin(url, urllink)
        time.sleep(3)



db = pymysql.connect(
        host = 'localhost',
        user = 'root',
        password = '',
        db = 'suumo_monthly_rent_development',
        charset = 'utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

for town_url in town_urls:
    scrape_from_suumo(town_url)

db.close()
