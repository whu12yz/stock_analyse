# -*- coding: utf-8 -*-
import threading
import datetime
import multiprocessing
import pandas as pd


class ClosingDif:
    def __init__(self):
        # self.work = 'all_stocks.xlsx'
        # self.new_work = 'all_stocks_pandas.xlsx'
        # self.work = 'min.xlsx'
        # self.new_work = 'min_pandas.xlsx'
        self.work = 'staging.xlsx'
        self.new_work = 'staging_pandas_new.xlsx'

    def read_excel(self):
        table = self.get_from_action_data(self.work)
        new_table = pd.DataFrame(index=table.index, columns=table.columns)
        self.domain(new_table, table)
        # self.multi_processing(new_table, table)
        new_table.to_excel(self.new_work)

    @staticmethod
    def get_from_action_data(fname):
        chunk_size=10000
        chunks = pd.read_excel(fname)
        return chunks

    def domain(self, new_table, table):
        for colnum in table.columns:
            print ('hello %s' % colnum)
            self.write_head(colnum, new_table, table)
            for rownum in table.index[252:]:
                row_date = rownum
                day = row_date.split('/')
                new_day = datetime.date(int(day[0]), int(day[1]), int(day[2]))
                fifty_date = (new_day - datetime.timedelta(days=350)).strftime('%Y/%m/%d')
                four_date = (new_day - datetime.timedelta(days=28)).strftime('%Y/%m/%d')
                fifty_new_date = self.get_date(fifty_date, table.index)
                four_dew_date = self.get_date(four_date, table.index)
                dif = self.count_dif(colnum, fifty_new_date, four_dew_date, row_date, table)
                new_table.set_value(row_date, colnum, dif)

    @staticmethod
    def write_head(colnum, new_table, table):
        new_table.ix[0, colnum] = table.ix[0, colnum]
        new_table.ix[1, colnum] = u'因子1'
        for index in table.index[2:252]:
            new_table.set_value(index, colnum, '--')

    @staticmethod
    def count_dif(colnum, fifty_new_date, four_new_date, rownum, table):
        if isinstance(table.ix[rownum, colnum], (int, float)):
            fifty_weeks_ago = table.ix[fifty_new_date, colnum]
            four_weeks_ago = table.ix[four_new_date, colnum]
            while not isinstance(fifty_weeks_ago, (int, float)) or not isinstance(four_weeks_ago, (int, float)):
                dif = '--'
                return dif
            dif = table.ix[rownum, colnum] / fifty_weeks_ago - table.ix[rownum, colnum] / four_weeks_ago
            dif = '%.4f' % dif
        else:
            dif = '--'
        return dif

    def multi_thread(self, new_table, table):
        threading_list = []
        for i in range(10):
            threading_list.append(
                threading.Thread(target = self.domain, args = (new_table, table,), name='pandas-thread-' + str(i))
            )
        for t in threading_list:
            t.start()
        for t in threading_list:
            t.join()

    def multi_processing(self, new_table, table):
        processing_list = []
        for i in range(4):
            processing_list.append(
                multiprocessing.Process(target = self.domain, args = (new_table, table,))
            )
        for t in processing_list:
            t.start()
        for t in processing_list:
            t.join()

    @staticmethod
    def get_date(date, date_list):
        while not (date in date_list):
            day = date.split('/')
            new_day = datetime.date(int(day[0]), int(day[1]), int(day[2]))
            date = (new_day - datetime.timedelta(days=1)).strftime('%Y/%m/%d')
        return date

if __name__ == '__main__':
    start = datetime.datetime.now()
    closing_dif = ClosingDif()
    closing_dif.read_excel()
    print (datetime.datetime.now() - start)

