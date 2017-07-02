# -*- coding: utf-8 -*-
import xlrd
import xlsxwriter
import threading
import datetime
import multiprocessing


def read_excel():
    data = xlrd.open_workbook('staging.xlsx')
    table = data.sheets()[0]
    file = xlsxwriter.Workbook('staging_new.xlsx')
    newTable = file.add_worksheet('Sheet1')
    style0 = file.add_format({'bold': True, 'num_format': '#,##0.0000'})
    dateList = table.col_values(0)
    domain(dateList, newTable, style0, table)
    file.close()


def domain(dateList, newTable, style0, table):
    for colnum in range(1, table.ncols):
        write_title(colnum, newTable, table)
        print ('hello %s' % colnum)
        for rownum in range(253, table.nrows):
            rowDate = table.row(rownum)[0]
            day = rowDate.value.split('/')
            newDay = datetime.date(int(day[0]), int(day[1]), int(day[2]))
            fiftyDate = (newDay - datetime.timedelta(days=350)).strftime('%Y/%m/%d')
            fourDate = (newDay - datetime.timedelta(days=28)).strftime('%Y/%m/%d')
            fiftyIndex = getDate(fiftyDate, dateList)
            fourIndex = getDate(fourDate, dateList)
            dif = count_dif(colnum, fiftyIndex, fourIndex, rownum, table)
            newTable.write(rownum, colnum, dif, style0)
            # print dif


def write_title(colnum, newTable, table):
    for i in range(0, 2):
        newTable.write(i, colnum, table.row(i)[colnum].value)
    newTable.write(2, colnum, u'因子1')
    for j in range(1, table.nrows):
        newTable.write(j, 0, table.row(j)[0].value)
    for i in range(3, 253):
        newTable.write(i, colnum, '--')


def count_dif(colnum, fiftyIndex, fourIndex, rownum, table):
    if isinstance(table.row(rownum)[colnum].value, (int, float)):
        fiftyWeeksAgo = table.row(fiftyIndex)[colnum].value
        fourWeeksAgo = table.row(fourIndex)[colnum].value
        # countFifty = 1
        while not isinstance(fiftyWeeksAgo, (int, float)):
            dif = '--'
            return dif
        while not isinstance(fourWeeksAgo, (int, float)):
            dif = '--'
            return dif
        dif = table.row(rownum)[colnum].value / fiftyWeeksAgo - table.row(rownum)[colnum].value / fourWeeksAgo
    else:
        dif = '--'
    return dif


def multi_thread(dateList, newTable, style0, table):
    threading_list = []
    for i in range(30):
        threading_list.append(
            threading.Thread(target=domain, args=(dateList, newTable, style0, table,), name='xlrd-thread-' + str(i))
        )
    for t in threading_list:
        t.start()
    for t in threading_list:
        t.join()


def multi_processing():
    processing_list = []
    for i in range(4):
        processing_list.append(
            multiprocessing.Process(target = read_excel, args = ())
        )
    for t in processing_list:
        t.start()
    for t in processing_list:
        t.join()


def getDate(date, list):
    # print date
    # print date in list
    while not (date in list):
        # print date
        day = date.split('/')
        newDay = datetime.date(int(day[0]), int(day[1]), int(day[2]))
        date = (newDay - datetime.timedelta(days=1)).strftime('%Y/%m/%d')
    # print list.index(date)
    return list.index(date)

start = datetime.datetime.now()
read_excel()
print datetime.datetime.now() - start

# multi_thread()
# multi_processing()
