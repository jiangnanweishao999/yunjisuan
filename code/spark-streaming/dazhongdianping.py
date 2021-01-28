# -*-coding:utf-8-*-
import re
import os
import json
import time
import gzip
import codecs
import random
import pyspark
import requests
import eventlet
import findspark
import subprocess
import multiprocessing
from bs4 import BeautifulSoup
from pyspark import SparkContext
from pyecharts import options as opts
from pyspark.streaming import StreamingContext
from pyecharts.charts import Pie, Page,Bar,Scatter
result=[]#record the all datas contains(shopName, mainRegionName, mainCategoryName, price, city, avgPrice)
USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
    "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5"]
head = {
'User-Agent': '{0}'.format(random.sample(USER_AGENT_LIST, 1)[0]),
}
#request header
cities=['上海','北京','南京','杭州','重庆','西安','成都','苏州','武汉','广州','深圳','天津']
def findFood(city,data):
    flag = 0
    for data in json.loads(data)["shopBeans"]:
        flag +=1
        mainCategoryName = data["mainCategoryName"]
        mainRegionName = data["mainRegionName"]
        tasteScore = str(data["refinedScore1"])
        environmentScore = str(data["refinedScore2"])
        serviceScore = str(data["refinedScore3"])
        shopName = data["shopName"]
        avgPrice = data["avgPrice"]
        if(mainRegionName!=None):
            params = shopName+","+mainRegionName+","+mainCategoryName+","+str((float(tasteScore)+float(environmentScore)+float(serviceScore))/3)+","+city+","+str(avgPrice)+"\n"
            print(params)#join the datas with ',', then show the datas
            result.append(params)   
    print("总条数：", flag)

def foodSpider(city_list):
    url = city_list[0]#rankId
    city = city_list[1]#city name
    base_url = "http://www.dianping.com/mylist/ajax/shoprank?"+url
    html = requests.get(base_url, headers=head)#ajax request, get json data
    findFood(city=city, data=str(html.text))#compile json data

def paint_pie(data,city):
	#[[region, count],[],[],...]
    page = Page(layout=Page.DraggablePageLayout)#new page
    pie1 = Pie().add(
            "",
            data,
            radius=["20%", "55%"],
            center=["30%", "80%"],
            rosetype="radius",
            label_opts=opts.LabelOpts(is_show=True,
            formatter="{b}: {c}  {per|{d}%} ",
            rich={
            "per": {
                    "color": "#eee",
                    "backgroundColor": "#334455",
                    "padding": [2, 4],
                    "borderRadius": 2,
                },
            }),
            
        ).set_global_opts(
        title_opts=opts.TitleOpts(
            title="Customized Pie",
            pos_left="center",
            pos_top="20",
            title_textstyle_opts=opts.TextStyleOpts(color="#fff"),
        ),
        legend_opts=opts.LegendOpts(type_="scroll", pos_left="80%", orient="vertical")
    )#various settings
    page.add(pie1)
    page.render(city+"饼图.html")#draw

def paint_Scatterplot(datas,city):
    #[[class,score,price],[],[]...]
    classes = []
    all_data=[]
    for i in datas:
        if i[0] not in classes:
            classes.append(i[0])#record different classes
    for i in classes:
        score=[]
        avgs=[]
        for j in datas:
            if j[0]==i:
                score.append(round(float(j[1]), 1))
                avgs.append(round(float(j[2]), 1))
        if(len(score)>=30):#data which number not large enough is useless
            all_data.append([i,score,avgs])
    page=Page()#new page
    scatter=Scatter(init_opts=opts.InitOpts(width="1000px", height="400px"))
    for i in all_data:
        scatter.add_xaxis(xaxis_data=i[2]).add_yaxis(
        series_name=i[0],
        y_axis=i[1],
        symbol_size=20,
        label_opts=opts.LabelOpts(is_show=False),
    ).set_series_opts().set_global_opts(
        xaxis_opts=opts.AxisOpts(
            type_="value", splitline_opts=opts.SplitLineOpts(is_show=True)
        ),
        yaxis_opts=opts.AxisOpts(
            type_="value",
            axistick_opts=opts.AxisTickOpts(is_show=True),
            splitline_opts=opts.SplitLineOpts(is_show=True),
        ),
        tooltip_opts=opts.TooltipOpts(is_show=False),
        visualmap_opts=opts.VisualMapOpts(
            type_="color", max_=10, min_=4, dimension=1
        ),
         datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
         legend_opts=opts.LegendOpts(type_="scroll", pos_left="90%", orient="vertical",selected_mode="single",),
    )#various settings
    page.add(scatter)
    page.render(city+"散点图.html")#draw
    
def main(sc):
    ssc=StreamingContext(sc,60)#set streaming context
    source_file = ssc.textFileStream("hdfs://master:9000/list/")#set monitor target
    source_file.foreachRDD(lambda x:start(x.collect(),sc))#start spider
    ssc.start()
    ssc.awaitTermination()
def start(target,sc):
    subprocess.call(["/home/hadoop/spark/hadoop-2.7.7/bin/hdfs","dfs","-rm","-r","/data.txt"])#to avoid error, delete it first
    list_city=[]
    if len(target)>=1:#avoid target is []
        city=""
        for line in target:
            line=re.sub('[\r\n]',"",line)
            if(line[0]!=' '):
                city=line
            else:
                list_city.append([re.sub('" class="BL"',"",re.findall(r'rankId.+"', line)[0]),city])
        #normalize the datas and group by city
        for city_data in list_city:
            foodSpider(city_data)#get detailed datas
        sc.parallelize(result).saveAsTextFile("hdfs://master:9000/data.txt")#save all data(consider of safety)
        lines=sc.textFile("hdfs://master:9000/data.txt")#read saved data
        splitlines=lines.map(lambda line:line.split(",")).filter(lambda line:len(line)==6)#split the data and filter [] and strange datas
        for city in cities:
            city_data=splitlines.map(lambda line:[line[1],line[4]]).filter(lambda line:line[1]==city).map(lambda line:[line[0],1]).reduceByKey(lambda x,y:x+y).sortBy(lambda line:line[1]).collect()
            city_data.reverse()#get data(region, count) of the city, group by region and sort by number
            paint_pie(city_data,city)#paint pie of each city
        for city in cities:
            city_data=splitlines.map(lambda line:[line[2],line[3],line[5]]).filter(lambda line:float(line[1])!=0 and float(line[2])!=0).collect()
            #get data(category, score, price) of the city, group by category
            paint_Scatterplot(city_data,city)#paint scatter plot of each city
