#数据转换成字典
def turn_to_dic(all):
    finaldic=[]
    list=['name','addr','type','score','city','price']
    for i in all:
        dic = dict(map(lambda x, y: [x, y], list, i))
        finaldic.append(dic)
    return finaldic
#选择城市，打开csv
def choosecity(cityname):
    all=[]
    error=0
    with open(cityname+'.csv','r',encoding='utf-8') as p:
       for i in p.readlines():
           try:
               temp=i[:-1].split(',')
               temp[3]=float(temp[3])
               temp[5]=int(temp[5])
               all.append(temp)
           except:
               error+=1
    return all[2:]
#选择餐饮类型
def choosetype(all):
    typelist=[]
    alltype=[]
    for i in all:
        if typelist.count(i['type'])==0:
            typelist.append(i['type'])
    for i in range(len(typelist)):
        temp=[typelist[i]]
        alltype.append(temp)
    for i in all:
        for k in range(len(typelist)):
            if len(i)>=5 and i['type']==typelist[k]:
                alltype[k].append(i)

    return [alltype,typelist]


#对list中地址相同的归一
def findalladdr(all):
    typelist = []
    alltype = []
    for i in range(1, len(all)):
        if typelist.count(all[i]['addr']) == 0:
            typelist.append(all[i]['addr'])
    return typelist


#选择商圈
def chooseaddr(all):
    typelist=[]
    alltype=[]
    for i in range(1,len(all)):
        if typelist.count(all[i]['addr'])==0:
            typelist.append(all[i]['addr'])
    for i in range(len(typelist)):
        temp=[typelist[i]]
        alltype.append(temp)
    for i in range(1,len(all)):
        for k in range(len(typelist)):
            if len(all[i])>=5 and all[i]['addr']==typelist[k]:
                alltype[k].append(all[i])
    for i in alltype:
        sum=0
        sumscore=0
        i.insert(1,len(i)-1)
        for k in range(2,len(i)):
            sum+=i[k]['price']
            sumscore+=i[k]['score']
        i.insert(2,sum/(len(i)-2))
        i.insert(2,sumscore/(len(i)-2))
    return [alltype,typelist]

#分别按价格、评分、平均分排序
def sortedbyprice(all):
    dic=sorted(all, key=lambda d: d['price'], reverse=True)
    return dic
def sortedbyscore(all):
    dic=sorted(all, key=lambda d: d['score'], reverse=True)
    return dic
def sortedbytypescore(all):
    torec=[]
    templen=[]
    temp=choosetype(all[4:])[0]
    for i in temp:
        sum = 0
        sumscore = 0
        i.insert(1, len(i) - 1)
        for k in range(2, len(i)):
            sum += i[k]['price']
            sumscore += i[k]['score']
        i.insert(2, sum / (len(i) - 2))
        i.insert(2, sumscore / (i[1]))
        templen.append(len(i))
    for i in temp:

        if i[1]>=8 or i[2]>=8.1:
            torec.append(i[0])
    return torec


#对二次分类后的list排序
def secondsort(alll):
    final=[]
    notinlist=[]
    for i in alll:
        temp=chooseaddr(i)
        final.append(temp[0])

    return [final,notinlist]




def tofindaddr():
    print('请选择您的目标城市')
    citylis=['上海','北京','南京','天津','广州','成都','杭州','武汉','深圳','苏州','西安','重庆']
    print(citylis)
    city=input()
    print('请选择您的餐饮类型')
    dict=turn_to_dic(choosecity(city))
    temp=choosetype(sortedbyprice(dict))
    alladdrlist=findalladdr(dict)

    print(temp[1])
    typee=input()
    index=temp[1].index(typee)
    sesorttemp=secondsort(temp[0])
    temp=sesorttemp[0][index]
    print('以下为同质餐饮商圈情况')
    for i in temp:
        print(i[:4])
        for k in i[4:]:
            print(k)
        print('---------------------------------------')
    print('以下为无同质餐饮商圈情况、商圈餐饮总数、均评分、人均消费')
    exist=[]
    cons=[]
    for i in temp:
        if exist.count(i[0])==0:
            exist.append(i[0])

    temp = sorted(chooseaddr(sortedbyprice(turn_to_dic(choosecity(city))))[0], key=lambda d: d[2], reverse=True)

    for i in alladdrlist:
        if i not in exist:
            cons.append(i)
    print(cons)
    ffff=[]
    for i in cons:

        print(temp[alladdrlist.index(i)][:4])
        print('---------------------------------------')





def toeat():
    print('请选择您的目标城市')
    citylis=['上海','北京','南京','天津','广州','成都','杭州','武汉','深圳','苏州','西安','重庆']
    print(citylis)
    city=input()
    print('商圈店铺总数、评分、人均、该商圈推荐类型如下')
    temp=sorted(chooseaddr(sortedbyprice(turn_to_dic(choosecity(city))))[0], key=lambda d: d[2], reverse=True)
    for i in temp:
        print(i[0:4])
        print(sortedbytypescore(i))
        print('---------------------------------------')


#入口
print('\'商户\'or\'顾客\'')
a=input()
if(a!='顾客'):
    tofindaddr()
    a=input()
else:
    toeat()
    a=input()