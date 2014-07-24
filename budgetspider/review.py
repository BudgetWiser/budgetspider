# -*- coding: utf-8 -*-
import os
from mongoengine import connect
from mongoengine.context_managers import switch_collection
from documents import Budgetspider


__rootdir = os.path.dirname(os.path.realpath(__file__)) + '/'
__mongoserver = "localhost"
__mongoport = 27017
__dbname = "budgetspider"
__colbudgetspider = "budgetspider"


# Lambda function for unicode UTF-8 encoding
def utf8 (x) : return x.encode('utf-8')


def calc_sum():
    connect(__dbname, host=__mongoserver, port=__mongoport)
    total = 0
    categories = [None]*40
    budgets = [0]*40
    with switch_collection(Budgetspider, __colbudgetspider) as CBudgetspider:
        for i in CBudgetspider.objects(year='2013'):
            name = (i['category_one'], i['category_two'])
            if name in categories:
                budgets[categories.index(name)] += i['budget_assigned']
                total += i['budget_assigned']
            else:
                for j in range(len(categories)):
                    if categories[j] == None:
                        categories[j] = name
                        break
                budgets[categories.index(name)] += i['budget_assigned']
                total += i['budget_assigned']

    for i in range(len(categories)):
        if not categories[i] == None:
            print categories[i][0].encode('utf-8'), categories[i][1].encode('utf-8'), budgets[i]
    print "TOTAL", total
    print "num_services", len(CBudgetspider.objects(year='2013'))


def get_service():
    connect(__dbname, host=__mongoserver, port=__mongoport)
    with switch_collection(Budgetspider, __colbudgetspider) as CBudgetspider:
        for i in CBudgetspider.objects(year='2013', category_one='일반공공행정', category_two='재정금융'):
            print utf8(i['category_three']), utf8(i['service']), i['budget_assigned']


# calc_sum()
get_service()
