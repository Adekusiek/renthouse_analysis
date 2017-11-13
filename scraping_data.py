from bs4 import BeautifulSoup
import urllib.request as req
from urllib.parse import urljoin
import pandas as pd
from pandas import Series, DataFrame
import time

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

    #Initialization for data column
    name = []
    address = []
    location = []
    age = []
    total_story = []
    floor_story = []
    floor = []
    rent = []
    admin = []
    others = []
    area = []

    #get data
    while urllink != None:
        urllink = None
        res = req.urlopen(url)
        soup = BeautifulSoup(res, "html.parser")

        contents = soup.select("#js-bukkenList .cassetteitem")
        for content in contents:
            for detail_content in content.select(".cassetteitem_other tbody"):
                # in case parking space is recorded
                if not '階' in detail_content.select("td")[2].string:
                    continue
                floor_story.append(detail_content.select("td")[2].string)
                rent.append(detail_content.select("td")[3].string)
                admin.append(detail_content.select("td")[4].string)
                others.append(detail_content.select("td")[5].string)
                floor.append(detail_content.select("td")[6].string)
                area.append(detail_content.select("td")[7].find(text=True))
                name.append(content.select_one(".cassetteitem_content-title").string)
                address.append(content.select_one(".cassetteitem_detail-col1").string)
                location.append(content.select(".cassetteitem_detail-col2 div")[0].string)
                age.append(content.select(".cassetteitem_detail-col3 div")[0].string)
                total_story.append(content.select(".cassetteitem_detail-col3 div")[1].string)


        # get link to next page
        rel_url_list = soup.select(".pagination_set  .pagination_set-nav > p")
        for rel_url in rel_url_list:
            if rel_url.a.string == "次へ":
                urllink = rel_url.a.attrs["href"]
        # get absolute path for next page
        url = urljoin(url, urllink)

        print("url:", url)
        time.sleep(5)

    print("No further page found")

    #各リストをシリーズ化
    name = Series(name)
    address = Series(address)
    location = Series(location)
    age = Series(age)
    total_story = Series(total_story)
    floor_story = Series(floor_story)
    rent = Series(rent)
    admin = Series(admin)
    others = Series(others)
    floor = Series(floor)
    area = Series(area)

    #各シリーズをデータフレーム化
    suumo_df = pd.concat([name, address, location, age, total_story, floor_story, rent, admin, others, floor, area], axis=1)

    #カラム名
    suumo_df.columns=['マンション名','住所','立地','築年数','建物高さ','階','賃料','管理費', '敷/礼/保証/敷引,償却','間取り','専有面積']

    #csvファイルとして保存
    filename = state_name + ".csv"
    suumo_df.to_csv(filename, sep = '\t',encoding='utf-16')

index = 0
for town_url in town_urls:
    index += 1
    if index <= 5:
        continue
    scrape_from_suumo(town_url)
    print(town_url[0], "is finished")
