# -*- coding: utf-8 -*-
import os
import re
from mongoengine import connect
from mongoengine.context_managers import switch_collection
from documents import Budgetspider, Cleanplus, Opengov


__rootdir = os.path.dirname(os.path.realpath(__file__)) + '/'
__mongoserver = "localhost"
__mongoport = 27017
__dbname = "budgetspider"
__colbudgetspider = "budgetspider"
__colcleanplus = "cleanplus"
__colopengov = "opengov"

connect(__dbname, host=__mongoserver, port=__mongoport)

# Lambda function for unicode UTF-8 encoding
def utf8 (x) : return x.encode('utf-8')


def calc_sum():
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
    with switch_collection(Budgetspider, __colbudgetspider) as CBudgetspider:
        for i in CBudgetspider.objects(year='2013', category_one='일반공공행정', category_two='재정금융'):
            print utf8(i['category_three']), utf8(i['service']), i['budget_assigned']

def find_service(_service_name):
    print 'budgetspider'
    with switch_collection(Budgetspider, __colbudgetspider) as CBudgetspider:
        for i in CBudgetspider.objects.all():
            if utf8(re.sub('[~*()\'\". -]', '', i['service'])) == utf8(re.sub('[~*()\'\". -]', '', _service_name)):
                for pr in map(utf8, (i['service'], i['year'], i['department'], i['category_one'], i['category_two'])):
                    print pr,
    print
    print 'opengov'
    with switch_collection(Opengov, __colopengov) as COpengov:
        for i in COpengov.objects():
            if utf8(re.sub('[~*()\'\". -]', '', i['name'])) == utf8(re.sub('[~*()\'\". -]', '', _service_name)):
                for pr in map(utf8, (i['name'], i['level_one'], u['level_two'])):
                    print pr,
    print
    print 'cleanplus'
    with switch_collection(Cleanplus, __colcleanplus) as CCleanplus:
        for i in CCleanplus.objects(service=_service_name):
            if utf8(re.sub('[~*()\'\". -]', '', i['service'])) == utf8(re.sub('[~*()\'\". -]', '', _service_name)):
                for pr in map(utf8, (i['service'], i['year'], i['department'])):
                    print pr,

# calc_sum()
#get_service()
find_service('서울 시니어예술제')
