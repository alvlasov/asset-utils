import os
import xlrd
import requests
import pathlib
import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup

class AssetDatabase:

    db_columns  = ['name', 'href', 'exchange', 'ticker', 'type']
    asset_types = ['etf', 'pif']
    domains     = { 'etf': 'http://world.investfunds.ru/etf',
                    'pif': 'http://pif.investfunds.ru/funds' }
    time_format = '%d.%m.%Y'

    def __init__(self):
        self._db = pd.DataFrame(columns = self.db_columns)
        self._asset_historicals = {}

    def retrieve_database(self):
        print('Retrieving database...')
        web_table_columns = { 'etf': { 'name': 0, 'exchange': 1, 'ticker': 2 },
                              'pif': { 'name': 0 } }
        web_page_token    = { 'etf': '/?p=',
                              'pif': '/?exclude_qualified=1&npage=' }
        empty_table_size = 2

        for asset_type in self.asset_types:
            page = 0
            while True:
                web_page = self.domains[asset_type] + web_page_token[asset_type] + str(page)
                r = requests.get(web_page)
                soup = BeautifulSoup(r.content, 'html.parser')
                asset_table = soup.find('table', id = 'funds_table')
                if asset_table == None:
                    break
                tr = asset_table.find_all('tr')
                if len(tr) <= empty_table_size:
                    break

                for elem in tr:
                    names = elem.find_all('td')
                    if len(names) != 0:
                        if asset_type == 'etf':
                            data = [names[web_table_columns[asset_type]['name']].string,
                                    names[web_table_columns[asset_type]['name']].a['href'].split('/')[-2],
                                    names[web_table_columns[asset_type]['exchange']].string,
                                    names[web_table_columns[asset_type]['ticker']].string,
                                    asset_type]
                        elif asset_type == 'pif':
                            data = [names[web_table_columns[asset_type]['name']].a.string,
                                    names[web_table_columns[asset_type]['name']].a['href'].split('/')[-1],
                                    float('nan'),
                                    float('nan'),
                                    asset_type]
                        self._db = self._db.append(pd.Series(data, index = self._db.columns), ignore_index = True)

                page += 1
        print('Success!')

    def save_database(self, name):
        self._db.to_csv('%s.csv' % name, index = False, encoding = 'utf-8')

    def load_database(self, name):
        new_db = pd.read_csv('%s.csv' % name)
        if len(set(new_db.columns).difference(self.db_columns)) != 0:
            print('Wrong file')
        else:
            self._db = new_db

    def find_in_database(self, token):
        return self._db[self._db['name'].str.contains(token, case = False) | (self._db['ticker'] == token)]

    def is_in_database(self, token):
        return any((self._db['name'] == token) | (self._db['ticker'] == token).tolist())

    def get_href(self, ticker_or_name):
        asset_type, href = self.get_info(ticker_or_name)
        if asset_type == None:
            return None
        else:
            return ('%s/%s') % (self.domains[asset_type], href)

    def get_info(self, ticker_or_name):
        result = self._db[(self._db['ticker'] == ticker_or_name) | (self._db['name'] == ticker_or_name)]
        try:
            return result.iloc[0]['type'], result.iloc[0]['href']
        except IndexError:
            return None, None

    def update_asset_historical(self, ticker_or_name):
        asset_type, asset_id = self.get_info(ticker_or_name)
        if asset_type != None:
            today = datetime.now()
            key = asset_type + str(asset_id)
            if key not in self._asset_historicals:
                start_date = datetime(1970, 1, 1)
                print('Downloading historical data for ' + ticker_or_name + '...')
                hist_df = self._retrieve_asset_historical(ticker_or_name, start_date, today)
                if type(hist_df) != None:
                    self._asset_historicals[key] = hist_df
                    print('Success!')
            else:
                start_date = self._asset_historicals[key].iloc[0]['date']
                print('Updating historical data (last was %s) for %s...' % (start_date.strftime(self.time_format), ticker_or_name))
                hist_df = self._retrieve_asset_historical(ticker_or_name, start_date, today)[:-1]
                if type(hist_df) != None:
                    if len(hist_df) == 0:
                        print('Up to date.')
                    else:
                        self._asset_historicals[key] = pd.concat([hist_df, self._asset_historicals[key]], ignore_index = True)
                        print('Success!')
        else:
            print('Asset %s not found' % ticker_or_name)

    def get_asset_historical(self, ticker_or_name, date = None):
        asset_type, asset_id = self.get_info(ticker_or_name)
        if asset_type != None:
            key = asset_type + str(asset_id)
            if key not in self._asset_historicals:
                self.update_asset_historical(ticker_or_name)
            if date == None:
                return self._asset_historicals[key]
            else:
                prices = self._asset_historicals[key][self._asset_historicals[key]['date'] <= date]
                try:
                    return prices.iloc[0]
                except IndexError:
                    print('No historical for %s at %s' % (ticker_or_name, date))
                    return None

        else:
            print('Asset %s not found' % ticker_or_name)
            return None

    def _retrieve_asset_historical(self, ticker_or_name, start_date, end_date):
        hist_columns = ['date', 'price']
        start_date_str = start_date.strftime(self.time_format)
        end_date_str = end_date.strftime(self.time_format)

        stats_df = pd.DataFrame(columns = hist_columns)
        asset_type, asset_id = self.get_info(ticker_or_name)

        if asset_type == 'etf':
            stats_page_href = '/stats'
            stats_page_table = { 'date' : 0, 'price' : 4 }
            empty_table_size = 2
            page = 0
            while True:
                href = self.get_href(ticker_or_name)
                params = { 'dateStart': start_date_str,
                           'dateEnd': end_date_str,
                           'p': page }
                r = requests.get(href + stats_page_href, params = params)
                soup = BeautifulSoup(r.content, 'html.parser')
                stats_table = soup.find('table', id = 'funds_table')
                tr = stats_table.find_all('tr')
                if len(tr) <= empty_table_size:
                    break;
                for elem in tr:
                    names = elem.find_all('td')
                    if len(names) != 0:
                        date = names[stats_page_table['date']].string
                        last_price_and_curr = names[stats_page_table['price']].string
                        if len(last_price_and_curr) > 1:
                            split = last_price_and_curr.split()
                            price_string = (''.join(split[:-1])).replace(' ', '')
                            last_price = float(price_string)
                        else:
                            last_price = float('nan')
                        data = [date, last_price]
                        stats_df = stats_df.append(pd.Series(data, index = hist_columns), ignore_index = True)
                page += 1

        elif asset_type == 'pif':
            export_page_href = self.domains[asset_type] + '/export_to_excel.php'
            params = { 'f2[0]':  asset_id,
                       'export' : '2',
                       'export_type' : 'xls',
                       'start_day' : start_date.day,
                       'start_month' : start_date.month,
                       'start_year' : start_date.year,
                       'finish_day' : end_date.day,
                       'finish_month' : end_date.month,
                       'finish_year' : end_date.year }
            r = requests.get(export_page_href, params = params, stream = True)
            if (r.status_code == 200) & (len(r.text) != 0):
                with open('_temp.xls', 'wb') as f:
                    for chunk in r.iter_content(1024):
                        f.write(chunk)
            else:
                print('Download error')
                return None
            wb = xlrd.open_workbook('_temp.xls', logfile = open(os.devnull, 'w'))
            stats_df = pd.read_excel(wb, engine = 'xlrd', skiprows = 3, header = None, names = ['date', 'price'], parse_cols = 2)
            os.remove('_temp.xls')

        elif asset_type == None:
            print('No such asset in the database')
            return None

        else:
            print('Unknown error')
            return None

        stats_df['date'] = stats_df['date'].apply(lambda x: datetime.strptime(x, self.time_format))
        return stats_df

    def save_asset_historicals(self, folder_name):
        pathlib.Path(folder_name).mkdir(parents = True, exist_ok = True)
        for key, value in self._asset_historicals.items():
            value.to_csv('%s/%s.csv' % (folder_name, key), index = False, encoding = 'utf-8')
        print('%d entries saved to %s' % (len(self._asset_historicals), folder_name))

    def load_asset_historicals(self, folder_name):
        path = pathlib.Path(folder_name)
        num = 0
        for fname in path.glob('*.csv'):
            df = pd.read_csv(fname, parse_dates = ['date'])
            key = str(fname).split('.')[-2].split('\\')[-1]
            self._asset_historicals[key] = df
            num += 1
        print('%d entries loaded from %s' % (num, folder_name))

class AssetPortfolio:

    # TODO update_position_historical
    # TODO зачем мне столько классов для позиций?

    class Position:
        def __init__(self, id, date, price, count, fee):
            self.id = id
            self.date = date
            self.price = price
            self.count = count
            self.fee = fee

    class OpenPosition(Position):
        def __init__(self, id, date, price, count, fee):
            super().__init__(id, date, price, count, fee)

    class ClosedPosition(Position):
        def __init__(self, id, date, price, count, fee):
            super().__init__(id, date, price, count, fee)

    class Fee():
        def __init__(self, date, fee):
            self.date = date
            self.fee = fee

    def __init__(self, database_file = None):
        self.asset_db = AssetDatabase()
        self.position_list = []
        if database_file == None:
            self.asset_db.retrieve_database()
        else:
            self.asset_db.load_database(database_file)

    def buy(self, id, date, price, count, fee):
        if self.asset_db.is_in_database(id):
            self.position_list.append(self.OpenPosition(id, np.datetime64(date), price, count, fee))
            self.position_list.sort(key = lambda x: x.date)
            #self.position_list.sort(key = lambda x:
            #                        datetime.strptime(x.date, self.asset_db.time_format))
        else:
            print('Asset ' + id + ' not found')

    def sell(self, id, date, price, count, fee):
        if self.asset_db.is_in_database(id):
            self.position_list.append(self.ClosedPosition(id, np.datetime64(date), price, count, fee))
            self.position_list.sort(key = lambda x: x.date)
            #self.position_list.sort(key = lambda x:
            #                        datetime.strptime(x.date, self.asset_db.time_format))
        else:
            print('Asset ' + id + ' not found')

    def pay_fee(self, date, fee):
        self.position_list.append(self.Fee(np.datetime64(date), fee))
        self.position_list.sort(key = lambda x: x.date)
        #self.position_list.sort(key = lambda x:
        #                        datetime.strptime(x.date, self.asset_db.time_format))

    def list_positions(self):
        for i, pos in zip(range(len(self.position_list)), self.position_list):
            if type(pos) == self.OpenPosition:
                print('%-3d %s\t %s\t buy\t %.2f\t %.2f\t %.2f' % (i, pos.id, pos.date, pos.price, pos.count, pos.fee))
            elif type(pos) == self.ClosedPosition:
                print('%-3d %s\t %s\t sell\t %.2f\t %.2f\t %.2f' % (i, pos.id, pos.date, pos.price, pos.count, pos.fee))
            elif type(pos) == self.Fee:
                print('%-3d Fee\t %s\t     \t %.2f' % (i, pos.date,  pos.fee))
            else:
                print('Unknown object')

    def delete_position(self, n):
        if (n < 0) | (n >= len(self.position_list)):
            print('Incorrect index')
        else:
            self.position_list.pop(n)

    def get_money_invested(self):
        result = 0
        state = [type(x) == self.OpenPosition for x in self.position_list]
        index = [i for i, x in enumerate(state) if x]
        for pos in [self.position_list[i] for i in index]:
            result += pos.price * pos.count
        return result

    def get_money_withdrawn(self):
        result = 0
        state = [type(x) == self.ClosedPosition for x in self.position_list]
        index = [i for i, x in enumerate(state) if x]
        for pos in [self.position_list[i] for i in index]:
            result += pos.price * pos.count
        return result

    def get_fee(self):
        result = 0
        for pos in self.position_list:
            result += pos.fee
        return result

    def get_portfolio_price(self, date):
        date = np.datetime64(date)
        price = 0
        for pos in self.position_list:
            if type(pos) == self.Fee:
                continue
            if pos.date > date:
                break
            pos_price = self.asset_db.get_asset_historical(pos.id, date)
            if type(pos_price) == None:
                print('Error')
                return None
            pos_price = pos_price['price']
            if type(pos) == self.OpenPosition:
                price += pos_price * pos.count
            elif type(pos) == self.ClosedPosition:
                price -= pos_price * pos.count
        return price

    def get_portfolio_price_span(self, start_date, end_date):
        time_format = self.asset_db.time_format
        date = np.datetime64(start_date)
        end_date = np.datetime64(end_date)
        delta = np.timedelta64(1, 'D')
        price = []
        while date <= end_date:
            current_price = self.get_portfolio_price(date)
            price.append((date, current_price))
            date += delta
        return pd.DataFrame(price, columns = ['date', 'price'])

    def get_portfolio_price_today(self):
        return self.get_portfolio_price(datetime.now())

    def get_profit(self):
        return self.get_portfolio_price_today() + self.get_money_withdrawn() - \
               self.get_fee() - self.get_money_invested()
