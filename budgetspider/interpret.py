# -*- coding: utf-8 -*-
import json
import os
import datetime
from mongoengine import connect
from mongoengine.context_managers import switch_collection
from documents import Cleanplus, Opengov, Budgetspider

# JSON files settings
__rootdir = os.path.dirname(os.path.realpath(__file__)) + '/'
__dirname = "json/"
#__cleanplus = "2013" # cp_XXXXXXXX
__opengov = "opengov"   # og_XXXXXXXX
__extension = ".json"

__cleanlist = ["20140719", "2013", "2012", "2011", "2010", "2009", "2008"]

# PyMongo DB settings
__mongoserver = "localhost"
__mongoport = 27017
__dbname = "budgetspider"
__colbudgetspider = "budgetspider"
__colopengov = "opengov"
__colcleanplus = "cleanplus"

def main():
    connect(__dbname, host=__mongoserver, port=__mongoport)
    cleanplus, opengov = [], []

    for __cleanplus in __cleanlist:
        # Read from cleanplus scraped json file
        with open(__rootdir + __dirname + __cleanplus + __extension) as f:
            cleanplus.extend(json.load(f))
            print 'Reading from', __dirname + __cleanplus + __extension, len(cleanplus)

    # Reduce duplcate entries
    quicksort(cleanplus, 0, len(cleanplus)-1) 
    rm_list = []
    for d in range(len(cleanplus)-1):
        if cleanplus[d] == cleanplus[d+1]:
            print 'DUPLICATE #1', cleanplus[d]
            print 'DUPLICATE #2', cleanplus[d+1]
            break
            rm_list.append(d)
        if not cleanplus[d]['index'].isdigit():
            # print 'NOTNUMBER   ', cleanplus[d]
            rm_list.append(d)
    rm_list.reverse()
    print "Handling", len(rm_list), "duplicate entries"
    for i in rm_list:
        del cleanplus[i]
    print 'Reduced to', len(cleanplus)

    # Read from opengov scraped json file
    with open(__rootdir + __dirname + __opengov + __extension) as f:
        opengov = json.load(f)
        print 'Reading from', __dirname + __opengov + __extension, len(opengov)

    # Log cleanplus DB entry
    with switch_collection(Cleanplus, __colcleanplus) as CCleanplus:
        num_saved, num_unsaved = 0, 0
        for c in cleanplus:
            if c['start_date'] == '':
                c['start_date'] = datetime.date(datetime.MINYEAR, 1, 1)
            if c['end_date'] == '':
                c['end_date'] = datetime.date(datetime.MINYEAR, 1, 1)
            if c['budget_assigned'] == '':
                c['budget_assigned'] = 0
            if c['budget_current'] == '':
                c['budget_current'] = 0
            if c['budget_contract'] == '':
                c['budget_contract'] = 0
            if c['budget_spent'] == '':
                c['budget_spent'] = 0

            data = CCleanplus(
                service = c['service'],
                year = c['year'],
                department = c['department'],
                team = c['team'],
                start_date = c['start_date'],
                end_date = c['end_date'],
                budget_summary = c['budget_summary'],
                budget_assigned = c['budget_assigned'],
                budget_current = c['budget_assigned'],
                budget_contract = c['budget_contract'],
                budget_spent = c['budget_spent']
            )

            try:
                data.save()
                num_saved += 1
            except:
                print data, len(CCleanplus.objects.all())
                print c['service'], c['year'], c['department'], c['team']
                print 'saved', num_saved


    # Log opengov DB entry
    with switch_collection(Opengov, __colopengov) as COpengov:
        for o in opengov:
            data = COpengov(
                service = o['name'],
                category_one = o['level_one'],
                category_two = o['level_two'],
                category_three = o['level_three']
            )

            try:
                data.save()
            except:
                print data, len(COpengov.objects.all())
                print c['service'], c['category_one'], c['category_two'], c['category_three']

    # Log budgetspider DB entry
    with switch_collection(Budgetspider, __colbudgetspider) as CBudgetspider:
        num_404, num_searched = 0, 0
        for c in cleanplus:
            found = False
            num_searched += 1
            for o in opengov:
                if o['name'] == c['service']:
                    found = True
                    data = CBudgetspider(
                        service = c['service'],
                        year = c['year'],
                        start_date = c['start_date'],
                        end_date = c['end_date'],
                        department = c['department'],
                        team = c['team'],
                        category_one = o['level_one'],
                        category_two = o['level_two'],
                        category_three = o['level_three'],
                        budget_summary = c['budget_summary'],
                        budget_assigned = c['budget_assigned'],
                        budget_current = c['budget_current'],
                        budget_contract = c['budget_contract'],
                        budget_spent = c['budget_spent']
                    )
                    try:
                        data.save()
                    except:
                        print data, len(CBudgetspider.objects.all())
                        print c['service'], c['category_one'], c['category_two'], c['category_three']
                    break
            if not found:
                err = "\t".join((c['service'], c['year'], c['department'], c['team'], c['budget_summary'])).encode('utf-8')
                print err
                num_404 += 1
        print num_404, "items are not matched"


# Quicksort implementation.
def quicksort(my_list, start, end):
    if start < end:
        pivot = _partition(my_list, start, end)
        quicksort(my_list, start, pivot-1)
        quicksort(my_list, pivot+1, end)
    return my_list

# Used for quicksort()
def _partition(my_list, start, end):
    pivot = int(my_list[start]['index'])
    left = start+1
    right = end
    done = False
    while not done:
        while left <= right and int(my_list[left]['index']) <= pivot:
            left += 1
        while int(my_list[right]['index']) >= pivot and right >= left:
            right -= 1
        if right < left:
            done = True
        else:
            temp = my_list[left]
            my_list[left] = my_list[right]
            my_list[right] = temp
    temp = my_list[start]
    my_list[start] = my_list[right]
    my_list[right] = temp
    return right

main()
