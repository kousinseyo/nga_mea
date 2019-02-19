import nga_spider
from nga_data_analy import get_comment, nga_wordcloud
import datetime
import pymysql

mea = nga_spider.nga()
mea.main(5900, 5900)
con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
cur = con.cursor()
cur.execute("select count(num) from mea_new where date<%s", datetime.datetime.now().strftime("%Y-%m-%d"))
height = cur.fetchall()[0][0]
print("楼层总高", height)
cur.execute("select count(num) from mea_new where date=date_sub(%s,interval 1 day)", datetime.datetime.now().strftime("%Y-%m-%d"))
height_increase = cur.fetchall()[0][0]
print("昨日楼增", height_increase)
cur.execute("select name,count(num) from mea_new where date=date_sub(%s,interval 1 day) group by name order by count(num) desc",
                datetime.datetime.now().strftime("%Y-%m-%d"))
result = cur.fetchall()
peoples = len(result)
people, total_comment = result[0]
print("昨日回帖人数", peoples)
print("昨天回帖最多的是%s,共回帖%d条" %(people, total_comment))
cur.execute("select name from mea_new where date<%s group by name", datetime.datetime.now().strftime("%Y-%m-%d"))
yesterday_people_total = len(cur.fetchall())
cur.execute("select name from mea_new where date<date_sub(%s,interval 1 day) group by name",
                datetime.datetime.now().strftime("%Y-%m-%d"))
the_day_before_yesterday_people_total = len(cur.fetchall())
yesterday_new_people = yesterday_people_total - the_day_before_yesterday_people_total
print("昨日新增", yesterday_new_people)
cur.execute("select num,praise from mea_new where date=date_sub(%s,interval 1 day) order by praise desc limit 10",
                datetime.datetime.now().strftime("%Y-%m-%d"))
praise = cur.fetchall()
print("点赞排行", praise)
data = get_comment(date=(datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d'))
nga_wordcloud(data, (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d'))
with open((datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')+".txt", "w") as f:
    f.write("[b]今日早间新闻[/b]\n昨日新增%d楼,共有%d人参与回帖,其中有%d个新人,昨天回帖最多的是%s,共回了%d贴.\n"
            % (height_increase, peoples, yesterday_new_people, people, total_comment))
    f.write("昨天获赞数最多的帖子Top10为\n[collapse=获赞最多的贴Top10]\n[table]\n[tr]\n[td]楼层号[/td]\n[td]帖子链接[/td]\n[td]获赞数[/td][/tr]\n")
    for p in praise:
        f.write("[tr]\n[td]%d[/td]\n[td][url]https://bbs.nga.cn/read.php?tid=15923050&page=%d#l%d[/url][/td]\n[td]%d[/td][/tr]\n"
                % (p[0], p[0]//20+1, p[0], p[1]))
    f.write("[/table]\n[/collapse]\n")
    f.write("[collapse=昨天的词云][/collapse]")
