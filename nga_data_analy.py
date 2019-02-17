import pymysql
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import jieba
import time
from wordcloud import WordCloud
from PIL import Image
from random import randint
import datetime
import re


# 楼层增长曲线图
def floor_increase():
    # 读取数据
    con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
    cur = con.cursor()
    cur.execute("select date,count(num) from mea_new where date<%s group by date", time.strftime("%Y-%m-%d"))
    result = list(cur.fetchall())
    data = pd.DataFrame(result, columns=["date", "number"])
    data["sum"] = data["number"].cumsum()
    cur.close()
    con.close()
    # 输出图形设置
    plt.rcParams["font.sans-serif"] = ["SimHei"]
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams['figure.dpi'] = 300
    # 绘图
    plt.title("楼层增长折线图")
    plt.ylabel("楼层数")
    plt.xlabel("日期")
    plt.grid(axis="y", linestyle=":")
    plt.plot(data["date"], data["sum"])
    plt.xticks(rotation=45)
    # plt.yticks(list(range(0, 110000, 10000)))
    plt.savefig("楼层增长折线图.png")
    plt.show()


# 楼层每日增长
def floor_increase_oneday():
    # 读取数据
    con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
    cur = con.cursor()
    cur.execute("select date,count(num) from mea_new where date<%s group by date", time.strftime("%Y-%m-%d"))
    result = list(cur.fetchall())
    data = pd.DataFrame(result, columns=["date", "number"])
    cur.close()
    con.close()
    # 输出图形设置
    plt.rcParams["font.sans-serif"] = ["SimHei"]
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams['figure.dpi'] = 300
    # 绘图
    plt.title("楼层每日增长")
    plt.ylabel("楼层数")
    plt.xlabel("日期")
    plt.grid(axis="y", linestyle=":")
    plt.bar(data["date"], data["number"])
    plt.xticks(rotation=45)
    # 找出盖楼最多的五天
    maxdate1 = data[["date", "number"]].groupby(by="number").min().iloc[-1]["date"]
    maxnum1 = data[["date", "number"]].groupby(by="number").min().iloc[-1].name
    maxdate2 = data[["date", "number"]].groupby(by="number").min().iloc[-2]["date"]
    maxnum2 = data[["date", "number"]].groupby(by="number").min().iloc[-2].name
    maxdate3 = data[["date", "number"]].groupby(by="number").min().iloc[-3]["date"]
    maxnum3 = data[["date", "number"]].groupby(by="number").min().iloc[-3].name
    maxdate4 = data[["date", "number"]].groupby(by="number").min().iloc[-4]["date"]
    maxnum4 = data[["date", "number"]].groupby(by="number").min().iloc[-4].name
    maxdate5 = data[["date", "number"]].groupby(by="number").min().iloc[-5]["date"]
    maxnum5 = data[["date", "number"]].groupby(by="number").min().iloc[-5].name
    # 绘制盖楼最多的五天的注解(需要根据实际情况手动调整文本及箭头位置)
    plt.annotate(str(maxdate1) + ":" + str(maxnum1), xy=(maxdate1, maxnum1), xycoords='data', xytext=(-20, 15),
                 textcoords='offset points', fontsize=10,
                 arrowprops=dict(arrowstyle='->', connectionstyle='arc3, rad=.2'))
    plt.annotate(str(maxdate2) + ":" + str(maxnum2), xy=(maxdate2, maxnum2), xycoords='data', xytext=(-10, 20),
                 textcoords='offset points', fontsize=10,
                 arrowprops=dict(arrowstyle='->', connectionstyle='arc3, rad=.2'))
    plt.annotate(str(maxdate3) + ":" + str(maxnum3), xy=(maxdate3, maxnum3), xycoords='data', xytext=(-40, 20),
                 textcoords='offset points', fontsize=10,
                 arrowprops=dict(arrowstyle='->', connectionstyle='arc3, rad=.2'))
    plt.annotate(str(maxdate4) + ":" + str(maxnum4), xy=(maxdate4, maxnum4), xycoords='data', xytext=(0, 10),
                 textcoords='offset points', fontsize=10,
                 arrowprops=dict(arrowstyle='->', connectionstyle='arc3, rad=.2'))
    plt.annotate(str(maxdate5) + ":" + str(maxnum5), xy=(maxdate5, maxnum5), xycoords='data', xytext=(-80, 30),
                 textcoords='offset points', fontsize=10,
                 arrowprops=dict(arrowstyle='->', connectionstyle='arc3, rad=.2'))
    plt.savefig("楼层每日增长图.png")
    plt.show()


# 回帖终端分布
def MobiePhone():
    client = {}
    # pc端
    con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
    cur = con.cursor()
    sql_pc = "select count(num) from mea_new where mobiephone=''"
    cur.execute(sql_pc)
    client["PC"] = cur.fetchall()[0][0]
    # Apple
    sql_apple = "select count(num) from mea_new where mobiephone like '%iPhone%' or mobiephone like '%iPad%'"
    cur.execute(sql_apple)
    client["Apple"] = cur.fetchall()[0][0]
    # huawei
    sql_huawei = "select count(num) from mea_new where mobiephone like '%HUAWEI%' or mobiephone='100 MHA-AL00' or mobiephone='100 VTR-AL00' or mobiephone='100 BKL-AL20'"
    cur.execute(sql_huawei)
    client["华为"] = cur.fetchall()[0][0]
    # xiaomi
    sql_xiaomi = "select count(num) from mea_new where mobiephone like '%Mi%' or mobiephone='100 HM NOTE 1LTE'"
    cur.execute(sql_xiaomi)
    client["小米"] = cur.fetchall()[0][0]
    # vivo
    sql_vivo = "select count(num) from mea_new where mobiephone like '%vivo%'"
    cur.execute(sql_vivo)
    client["vivo"] = cur.fetchall()[0][0]
    # oneplus
    sql_oneplus = "select count(num) from mea_new where mobiephone like '%oneplus%'"
    cur.execute(sql_oneplus)
    client["一加"] = cur.fetchall()[0][0]
    # sony
    sql_sony = "select count(num) from mea_new where mobiephone like '%Sony%' or mobiephone='100 F8332' or mobiephone='100 G8342'"
    cur.execute(sql_sony)
    client["Sony"] = cur.fetchall()[0][0]
    # samsung
    sql_samsung = "select count(num) from mea_new where mobiephone like '%samsung%'"
    cur.execute(sql_samsung)
    client["三星"] = cur.fetchall()[0][0]
    # oppo
    sql_oppo = "select count(num) from mea_new where mobiephone like '%OPPO%'"
    cur.execute(sql_oppo)
    client["OPPO"] = cur.fetchall()[0][0]
    # HMD
    sql_hmd = "select count(num) from mea_new where mobiephone like '%HMD%'"
    cur.execute(sql_hmd)
    client["HMD"] = cur.fetchall()[0][0]
    # 不明
    sql_unknown = "select count(num) from mea_new where mobiephone='8' or mobiephone='7'"
    cur.execute(sql_unknown)
    client["不明"] = cur.fetchall()[0][0]
    # 其他
    cur.execute("select count(num) from mea_new")
    total_num = cur.fetchall()[0][0]
    for i in client:
        total_num -= client[i]
    client["其他"] = total_num
    client = pd.DataFrame(pd.Series(client), columns=["发帖数"])
    client = client.reset_index().rename(columns={"index": "终端"})
    client = client.sort_values("发帖数", ascending=False)
    cur.close()
    con.close()
    # 输出图形设置
    plt.rcParams["font.sans-serif"] = ["SimHei"]
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams['figure.dpi'] = 300
    # 绘图
    plt.title("发帖使用终端分布")
    plt.pie(client["发帖数"], labels=client["终端"], autopct="%d%%", shadow=False, startangle=90,labeldistance=1.12,
            textprops={'fontsize': 4.5, 'color': 'black'})
    plt.axis("equal")
    plt.savefig("发帖使用终端分布图.png")
    plt.show()


# 各时段回帖数量
def floor_increase_onehour():
    # 读取数据
    con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
    cur = con.cursor()
    cur.execute("select date_format(time,'%H%M%S') hours,count(num) from mea_new group by hours")
    hour = cur.fetchall()
    hour_data = pd.DataFrame(list(hour), columns=["hour", "num"])
    cur.close()
    con.close()
    # 输出图形设置
    plt.rcParams["font.sans-serif"] = ["SimHei"]
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams['figure.dpi'] = 300
    # 绘图
    plt.title("各时段发帖数")
    plt.ylabel("发帖数")
    plt.xlabel("时间")
    plt.grid(axis="y", linestyle=":")
    x = np.arange(24)
    hours = plt.bar(x, hour_data["num"])
    plt.xticks(x)
    for hour in hours:
        h = hour.get_height()
        plt.text(hour.get_x() + hour.get_width() / 2, h, "%d" % int(h), ha="center", va="bottom", fontsize=6.6)
    plt.savefig("各时段发帖数.png")
    plt.show()


# 回帖数量分析
def tie_stat():
    # 读取数据
    con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
    cur = con.cursor()
    # 回帖数Top30
    cur.execute("select name,count(num) from mea_new group by name order by count(num) desc limit 30")
    name_num = cur.fetchall()
    name_num = pd.DataFrame(list(name_num), columns=["昵称", "发帖数"])
    print("回帖数Top30")
    print(name_num)

    # 总回帖人数
    cur.execute("select name,count(num) from mea_new group by name")
    name_count = len(cur.fetchall())
    print("共有%d人参与回帖" % name_count)

    # 总贴数
    cur.execute("select count(num) from mea_new")
    tie_count = cur.fetchall()[0][0]
    print("共回复有%d贴" % tie_count)

    # 回帖数前15%人发帖总数
    cur.execute("select name,count(num) from mea_new group by name order by count(num) desc limit %s",
                int(0.15 * name_count))
    count15 = 0
    for i in cur.fetchall():
        count15 += i[1]
    print("回帖数前15%%的人回帖数占总回帖数的%.2f%%" % (count15/tie_count*100))
    cur.close()
    con.close()


# 绘制词云
def nga_wordcloud(result, filename):
    jieba.load_userdict("nga.txt")  # 载入自定义分词词典
    word_list = []
    # 分词
    for i in result:
        comment = re.sub("(\[b\].*?\[\/b\])|(\[s:ac.*?\])|(\[quote\].*?\[\/quote\])|(\[img\].*?\[\/img\])", "", i[0])
        word_list += jieba.cut(comment)
    # 读取自定义过滤词
    with open("FilterWords.txt") as f:
        filter_words = f.read().split("分隔符")
    # 过滤分词结果
    word_list = [i for i in word_list if (i not in filter_words)]
    # 打印出现率Top200的词及词频
    print(dict(pd.Series(word_list).value_counts().head(200)))
    # 输出图形设置
    plt.rcParams["font.sans-serif"] = ["SimHei"]
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams['figure.dpi'] = 300
    # 绘制词云
    word_str = ",".join(word_list)
    cloud_mask = np.array(Image.open("./img/meaqua.jpg"))  # 读取绘制词云参考的图像
    # 定义词云颜色参考函数
    def random_color_func2(word=None, font_size=None, position=None, orientation=None, font_path=None, random_state=None):
        h = randint(175, 220)
        s = int(100.0 * 255.0 / 255.0)
        l = int(100.0 * float(randint(90, 150)) / 255.0)
        return "hsl({}, {}%, {}%)".format(h, s, l)
    wordcloud = WordCloud(scale=5, font_path="C:\Windows\Fonts\simkai.ttf", width=1920, height=1080, background_color="white",
                          color_func=random_color_func2, mask=cloud_mask, colormap="tab20").generate(word_str)
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.savefig(filename+".png")
    plt.show()


# 获取回帖内容
def get_comment(date=None, name=None):
    # 读取数据
    con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
    cur = con.cursor()
    if (date is None) and (name is None):
        cur.execute("select comment from mea_new")
    elif (date is None) and (name is not None):
        cur.execute("select comment from mea_new where name=%s", name)
    elif (date is not None) and (name is None):
        cur.execute("select comment from mea_new where date=%s", date)
    else:
        raise Exception("参数输入错误")
    result = cur.fetchall()
    cur.close()
    con.close()
    return result


# 绘制总词云
def total_wordcloud():
    result = get_comment()
    nga_wordcloud(result, "总词云")


# 水贴之王
def water():
    con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
    cur = con.cursor()
    # 各时间段水贴之王
    sql = "select name,count(num) from mea_new where time between %s and %s group by name order by count(num) desc limit 1"
    time_tie = []
    for i in range(24):
        cur.execute(sql, ["{:02d}".format(i)+":00:00", "{:02d}".format(i)+":59:59"])
        re = cur.fetchall()[0]
        time_tie.append([i, re[0], re[1]])
    time_tie = pd.DataFrame(time_tie, columns=["时间", "昵称", "发帖数"])
    print("各时间段水贴之王")
    print(time_tie)

    # 每日水贴之王
    sql = "select name,count(num) from mea_new where date=%s group by name order by count(num) desc limit 1"
    d = np.datetime64("2018-12-17")
    d_end = np.datetime64("2019-02-17")
    date_tie = []
    while d < d_end:
        cur.execute(sql, str(d))
        re = cur.fetchall()[0]
        date_tie.append([d, re[0], re[1]])
        d += 1
    date_tie = pd.DataFrame(date_tie, columns=["日期", "昵称", "发帖数"])
    print("每日水贴之王")
    print(date_tie)
    cur.close()
    con.close()


# 回帖人数和新人人数
def new_man():
    # 获取数据
    con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
    cur = con.cursor()
    sql = "select name from mea_new where date=%s group by name"
    nowtime = datetime.datetime.now()
    nowdate = np.datetime64(nowtime.date())
    startdate = np.datetime64("2018-12-17")
    date_tie = []
    man = [] # 参与过回帖的人昵称列表
    while startdate < nowdate:
        cur.execute(sql, str(startdate))
        result = cur.fetchall()
        newman = [i[0] for i in result if i[0] not in man] # 新人昵称列表
        man += newman
        # print(startdate, len(newman), len(man))
        date_tie.append([startdate, len(result), len(newman)])
        startdate += 1
    date_tie = pd.DataFrame(date_tie, columns=["date", "sum", "new"])
    cur.close()
    con.close()
    # 输出图形设置
    plt.rcParams["font.sans-serif"] = ["SimHei"]
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams['figure.dpi'] = 300
    # 绘图
    plt.title('每日发帖人数及新增的新人', fontsize=20)
    # 设置坐标轴标签
    plt.xlabel('日期', fontsize=14)
    plt.ylabel('人数', fontsize=14)
    # 设置刻度参数
    plt.tick_params(labelsize=10)
    # 设置网格线型
    plt.grid(axis='y', linestyle=':')
    # 绘制条形图
    x = np.arange(len(date_tie))
    plt.bar(date_tie["date"], date_tie["sum"], 0.4, color='red', label='回帖人数')
    plt.bar(date_tie["date"] + np.timedelta64(600, "m"), date_tie["new"], 0.4, color='blue', label='新人人数',
                    alpha=0.75)
    # 设置水平坐标刻度标签
    plt.xticks(rotation=60)
    plt.legend()
    # 显示图形
    plt.savefig("每日发帖人数及新增新人.png")
    plt.show()


# 注册日期统计
def RegDate():
    con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
    cur = con.cursor()
    # 楼内成员注册最早top10
    cur.execute('select name,regdate from mea_new where name !="匿名" group by name order by regdate limit 10')
    reg = cur.fetchall()
    reg = pd.DataFrame(list(reg), columns=["昵称", "注册时间"])
    print("楼内成员注册最早Top10")
    print(reg)
    # 绘制楼内成员注册年份分布图
    cur.execute("select date_format(regdate,'%Y') year from mea_new group by name")
    regdate = [i[0] for i in cur.fetchall()]
    regdate = pd.Series(regdate)
    counts = regdate.value_counts().sort_index()
    plt.title("注册年份分布图")
    plt.ylabel("人数")
    plt.xlabel("年份")
    plt.grid(axis="y", linestyle=":")
    years = plt.bar(counts.index, counts.values)
    for year in years:
        y = year.get_height()
        plt.text(year.get_x() + year.get_width() / 2, y, "%d"%int(y), ha="center", va="bottom", fontsize=10)
    plt.savefig("注册年份分布图.png")
    plt.show()


# 点赞统计
def praise():
    con = pymysql.connect("localhost", "root", "123456", "nga", charset="utf8")
    cur = con.cursor()
    # 获赞最多的贴top10
    cur.execute('select name,num,praise from mea_new order by praise desc limit 10')
    reg = cur.fetchall()
    reg = pd.DataFrame(list(reg), columns=["昵称", "楼层号", "获赞数"])
    print("获赞数最多的贴Top10")
    print(reg)
    # 获赞最多的人top30
    cur.execute('select name,sum(praise),count(num),sum(praise)/count(num) from mea_new group by name order by sum(praise) desc limit 30')
    reg = cur.fetchall()
    reg = pd.DataFrame(list(reg), columns=["昵称", "获赞数", "回帖数", "获赞/回帖"])
    print("获赞最多的人Top30")
    print(reg)
    cur.close()
    con.close()


if __name__ == "__main__":
    # data = get_comment(name="Akiyo桑")
    # nga_wordcloud(data,"Akiyo桑")