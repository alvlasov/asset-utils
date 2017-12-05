import os
import xlrd
import requests
import pathlib
import pandas as pd
import numpy as np
import logging
import pickle
from datetime import datetime, date
from bs4 import BeautifulSoup

class AssetDatabase:

    db_columns  = ['name', 'exchange', 'ticker', 'href', 'type']
    asset_types = ['etf', 'pif']
    domains     = {'etf': 'http://world.investfunds.ru/etf',
                   'pif': 'http://pif.investfunds.ru/funds'}
    time_format = '%d.%m.%Y'

    def __init__(self):
        self._db = pd.DataFrame(columns = self.db_columns)

    def retrieve_database(self):
        new_db = pd.DataFrame(columns = self.db_columns)
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
                            href = names[web_table_columns[asset_type]['name']].a['href'].split('/')[-2]
                            data = [names[web_table_columns[asset_type]['name']].string,
                                    names[web_table_columns[asset_type]['exchange']].string,
                                    names[web_table_columns[asset_type]['ticker']].string,
                                    href,
                                    asset_type]
                        elif asset_type == 'pif':
                            href = names[web_table_columns[asset_type]['name']].a['href'].split('/')[-1]
                            data = [names[web_table_columns[asset_type]['name']].a.string,
                                    float('nan'),
                                    float('nan'),
                                    href,
                                    asset_type]
                        id = str(asset_type + str(href))
                        new_db = new_db.append(pd.Series(data, index = self._db.columns, name = id))
                page += 1

        self._db = new_db
        print('Success!')

    def save_database(self, name):
        self._db.to_csv('%s.csv' % name, encoding = 'utf-8')

    def load_database(self, name):
        new_db = pd.read_csv('%s.csv' % name, index_col = 0)
        if len(set(new_db.columns).difference(self.db_columns)) != 0:
            raise ValueError('Wrong database file')
        else:
            self._db = new_db

    def find(self, token):
        mask = np.column_stack([self._db[col].astype(str).str.contains(token, case = False,
                                                   na = False) for col in self._db.columns])
        return self._db.iloc[mask.any(axis = 1)]

    def get_entry(self, id):
        try:
            return self._db.loc[id]
        except KeyError:
            raise LookupError('Entry is not in the database')

    def is_in_database(self, id):
        try:
            self.get_entry(id)
            return True
        except LookupError:
            return False

    def retrieve_asset_historical(self, id, start_date, end_date):
        hist_columns = ['date', 'price']
        start_date_str = start_date.strftime(self.time_format)
        end_date_str = end_date.strftime(self.time_format)

        stats_df = pd.DataFrame(columns = hist_columns)
        asset_type, asset_href = self.get_entry(id).loc[['type', 'href']]

        if asset_type == 'etf':
            stats_page_href = '/stats'
            stats_page_table = { 'date' : 0, 'price' : 4 }
            empty_table_size = 2
            page = 0
            while True:
                href = '%s/%s' % (self.domains[asset_type], asset_href)
                params = { 'dateStart': start_date_str,
                           'dateEnd': end_date_str,
                           'p': page }
                r = requests.get(href + stats_page_href, params = params)
                soup = BeautifulSoup(r.content, 'html.parser')
                stats_table = soup.find('table', id = 'funds_table')
                tr = stats_table.find_all('tr')
                if len(tr) <= empty_table_size:
                    break
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
            params = { 'f2[0]':  asset_href,
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
            stats_df = pd.concat([stats_df, pd.read_excel(wb, engine = 'xlrd', skiprows = 3, header = None,
                                            names = ['date', 'price'], parse_cols = 1)])
            os.remove('_temp.xls')
        stats_df['date'] = pd.to_datetime(stats_df['date'], format = self.time_format)

        return stats_df

class Asset:

    def __init__(self, id, db, start_date = '1970-01-01'):
        self.last_updated = pd.to_datetime(start_date) - pd.Timedelta(days = 1)
        self._db = db
        self.id = id
        self.description =  self._db.get_entry(self.id)
        self.name, self.ticker = self.description.loc[['name', 'ticker']]
        self.stats = pd.DataFrame(columns = ['count'])

    def update(self):
        today = pd.to_datetime(date.today())
        if today != self.last_updated:
            date_range = pd.date_range(start = self.last_updated + pd.Timedelta(days = 1), end = today)

            if len(self.stats) == 0:
                init_value = 0
            else:
                init_value = self.stats.iloc[-1][0]

            stats_df = pd.DataFrame(init_value, index = date_range, columns = ['count'])
            prices_df = self._db.retrieve_asset_historical(self.id, self.last_updated, today)

            price = []
            for s in date_range:
                prices = prices_df[prices_df['date'] <= s]
                if len(prices) != 0:
                    price.append(prices.iloc[0]['price'])
                else:
                    price.append(self.stats.iloc[-1]['price'])

            stats_df['price'] = pd.Series(price, index = date_range)

            self.stats = self.stats.append(stats_df)
            logging.info('Asset "%s" updated from %s' % (self.id, self.last_updated))
            self.last_updated = today
        else:
            logging.info('Asset "%s" is up date' % self.id)

    def add(self, date_, count):
        for idx in self.stats.index[self.stats.index >= date_]:
            self.stats.loc[idx, 'count'] += count
            if self.stats.loc[idx, 'count'] < 0:
                raise ValueError("Asset count couldn't be less than zero")

    def get_price(self, date_):
        return self.stats.loc[date_]['price']

    def get_count(self, date_):
        return self.stats.loc[date_]['count']

    def __eq__(self, other):
        return (self.id == other.id)

class AssetPortfolio:

    class Position:
        def __init__(self, asset, date, price, count, fee):
            self.asset = asset
            self.date = date
            self.price = price
            self.count = count
            self.fee = fee
            self.asset.add(self.date, self.count)

        def __del__(self):
            self.asset.add(self.date, -self.count)

        def type(self):
            if self.count > 0:
                return 'open'
            else:
                return 'close'

    class Fee():
        def __init__(self, date, fee):
            self.date = date
            self.fee = fee
        def type(self):
            return 'fee'

    def __init__(self, db, creation_date):
        self.creation_date = pd.to_datetime(creation_date)
        self.last_updated = self.creation_date
        self.asset_db = db
        self.asset_list = []
        self.position_list = []

    def add_asset(self, id):
        new_asset = Asset(id, self.asset_db, self.creation_date)
        if new_asset not in self.asset_list:
            self.asset_list.append(new_asset)
            logging.info('Asset %s added to portfolio.asset_list' % id)
        else:
            logging.info('Asset %s is already in portfolio.asset_list' % id)

    def remove_asset(self, id):
        asset = Asset(id, self.asset_db)
        if asset in self.asset_list:
            new_pos_list = []
            for pos in self.position_list:
                if type(pos) == self.Fee:
                    new_pos_list.append(pos)
                elif pos.asset != asset:
                    new_pos_list.append(pos)
            self.position_list = new_pos_list
            self.asset_list.remove(asset)
            logging.info('Asset %s and all corresponding positions are removed from portfolio.asset_list' % id)

        else:
            logging.info('Asset %s is not in portfolio.asset_list' % id)

    def _add_position(self, id, date, price, count, fee):
        date = pd.to_datetime(date)
        if date < self.creation_date:
            raise ValueError('Date is earlier than portfolio creation date!')
        if date > self.last_updated:
            raise ValueError('Date is greater than portfolio last update date!')
        asset = Asset(id, self.asset_db)
        if asset in self.asset_list:
            idx = self.asset_list.index(asset)
            position = self.Position(self.asset_list[idx], pd.to_datetime(date), price, count, fee)
            self.position_list.append(position)
            self.position_list.sort(key = lambda x: x.date)
            logging.info('Position (%s) (%s, %s, %f, %f, %f) added' % (position.type(), id, date, price, count, fee))
        else:
            logging.info('Asset %s is not in portfolio.asset_list' % id)

    def buy(self, id, date, price, count, fee):
        self._add_position(id, date, price, count, fee)

    def sell(self, id, date, price, count, fee):
        self._add_position(id, date, price, -count, fee)

    def pay_fee(self, date, fee):
        date = pd.to_datetime(date)
        if date < self.creation_date:
            raise ValueError('Date is earlier than portfolio creation date!')
        self.position_list.append(self.Fee(pd.to_datetime(date), fee))
        self.position_list.sort(key = lambda x: x.date)
        logging.info('Fee (%s, %f) paid' % (date, fee))

    def remove_position(self, n):
        self.position_list.pop(n)
        logging.info('Position %d removed' % n)

    def get_position_list(self):
        pos_list = []
        for pos in self.position_list:
            p_type = pos.type()
            if p_type == 'open':
                pos_list.append([pos.asset.id, pos.date, 'buy', pos.price, pos.count, pos.fee])
            elif p_type == 'close':
                pos_list.append([pos.asset.id, pos.date, 'sell', pos.price, -pos.count, pos.fee])
            elif p_type == 'fee':
                pos_list.append(['fee', pos.date, float('nan'), float('nan'), float('nan'), pos.fee])
        return pd.DataFrame(pos_list, columns = ['Name', 'Date', 'Type', 'Price', 'Count', 'Fee'])

    def get_asset_list(self):
        asset_list = []
        for asset in self.asset_list:
            asset_list.append(asset.description)
        return pd.DataFrame(asset_list)

    def update(self):
        today = pd.to_datetime(date.today())
        if today != self.last_updated:
            for asset in self.asset_list:
                asset.update()
            logging.info('Portfolio updated from %s' % self.last_updated)
            self.last_updated = today
        else:
            logging.info('Portfolio is up to date')

    def get_price(self, date_):
        price = 0
        for asset in self.asset_list:
            price += asset.get_count(date_) * asset.get_price(date_)
        return price

    def get_alltime_stats(self):
        opened = 0
        closed = 0
        fee = 0
        date_ = self.last_updated
        for pos in self.position_list:
            p_type = pos.type()
            if p_type == 'open':
                opened += pos.count * pos.price
            elif p_type == 'close':
                closed += -pos.count * pos.price
            fee += pos.fee
        p_price = self.get_price(date_)
        profit = p_price + closed - fee - opened
        return pd.DataFrame([[self.creation_date, date_, opened, closed, fee, p_price, profit, profit/opened*100]],
                            columns = ['Start date', 'End date', 'Invested', 'Closed', 'Fees',
                                       'Current portfolio price', 'Result', 'Result %'])

    def get_stats(self, time_offset):

        def get_state(date_):
            state = {}
            for asset in self.asset_list:
                state[asset.name] = asset.get_price(date_)
            state['Portfolio'] = self.get_price(date_)
            return state

        date = pd.to_datetime(self.last_updated)
        prev_date = date

        state = get_state(date)

        states = []
        dates = []
        invest = []

        end = False
        while not end:

            prev_date -= time_offset
            if prev_date <= self.creation_date:
                prev_date = self.creation_date
                end = True
            prev_state = get_state(prev_date)

            opened = 0
            closed = 0
            for pos in self.position_list:
                if pos.date < prev_date: continue
                if pos.date >= date: break
                p_type = pos.type()
                if p_type == 'open':
                    opened += pos.count * pos.price
                elif p_type == 'close':
                    closed += -pos.count * pos.price

            adj_result = ''
            for key in prev_state.keys():
                if prev_state[key] != 0:
                    if key == 'Portfolio':
                        adj_result = '%+.2f' % ((state[key] + closed - opened - prev_state[key]) / prev_state[key] * 100)
                    state[key] = str(state[key]) + ' (%+.2f)' % ((state[key] - prev_state[key]) / prev_state[key] * 100)

            invest.append(pd.Series([adj_result, opened, closed], index = ['Adjusted portfolio result', 'Invested', 'Closed']))
            dates.append(pd.Series([prev_date, date], index = ['Start date',  'End date']))
            states.append(state)

            date = prev_date
            state = prev_state

        stats_df = pd.DataFrame.from_dict(states)
        portfolio_df = stats_df['Portfolio']
        stats_df = stats_df.drop(['Portfolio'], axis = 1)
        return pd.concat([pd.DataFrame(dates), stats_df, portfolio_df, pd.DataFrame(invest)], axis = 1)

    def get_weekly_stats(self):
        return self.get_stats(pd.offsets.Week(weekday = 0))

    def get_monthly_stats(self):
        return self.get_stats(pd.offsets.MonthBegin())

    def get_annual_stats(self):
        return self.get_stats(pd.offsets.YearBegin())

    def save(self, name):
        data = [self.creation_date, self.last_updated, self.asset_db, self.asset_list, self.position_list]
        with open('%s.pkl' % name, 'wb') as f:
            pickle.dump(data, f)
        logging.info('Portfolio saved to %s.pkl' % name)

    def load(self, name):
        with open('%s.pkl' % name, 'rb') as f:
            data = pickle.load(f)
        self.creation_date = data[0]
        self.last_updated = data[1]
        self.asset_db = data[2]
        self.asset_list = data[3]
        self.position_list = data[4]
        logging.info('Portfolio loaded from %s.pkl' % name)
        return self

    def get_asset_counts(self, date_ = None):
        if date_ == None:
            date_ = self.last_updated
        counts = {}
        price = self.get_price(date_)
        for asset in self.asset_list:
            count = asset.get_count(date_)
            asset_price = asset.get_price(date_)
            counts[asset.name] = [count]
        counts['Date'] = [date_]
        return pd.DataFrame.from_dict(counts)

    def get_distribution(self, date_ = None):
        if date_ == None:
            date_ = self.last_updated
        counts = {}
        price = self.get_price(date_)
        for asset in self.asset_list:
            count = asset.get_count(date_)
            asset_price = asset.get_price(date_)
            counts[asset.name] = [count*asset_price/price]
        counts['Date'] = [date_]
        return pd.DataFrame.from_dict(counts)

import cvxpy

class Rebalancer:

    def rebalance(self, portfolio, target_distr, refill = 0):
        n_assets = len(portfolio.asset_list)
        if len(target_distr) != n_assets:
            raise Exception('Wrong len of target_split')
        if sum(target_distr) != 1:
            raise Exception('Wrong partitioning')

        date = portfolio.last_updated
        portfolio_price = portfolio.get_price(date)
        target_price = portfolio_price + refill
        asset_prices = []
        assets_with_real_counts = []
        for i, asset in enumerate(portfolio.asset_list):
            asset_prices.append(asset.get_price(date))
            if asset.get_count(date) % 1 != 0:
                assets_with_real_counts.append(i)

        real_multiplier = 10 ** 6
        for i in assets_with_real_counts:
            asset_prices[i] /= real_multiplier

        A = np.diag(asset_prices/target_price)
        y = np.array(target_distr)
        x = cvxpy.Int(n_assets)
        obj = cvxpy.Minimize(cvxpy.norm(A * x - y, 2))
        prob = cvxpy.Problem(obj)
        sol = prob.solve()
        x_val = np.array(x.value).reshape(-1,).tolist()

        for i in assets_with_real_counts:
            x_val[i] /= real_multiplier

        eps = 10 ** -6
        output = []
        output_cols = ['Name', 'Price', 'Count', 'Price*Count', 'Rebalanced count', 'Price*RCount', 'Price*(RCount-Count)', 'Tip', 'Distribution', 'Rebalanced distribution']
        for i in range(n_assets):
            asset = portfolio.asset_list[i]
            price = asset.get_price(date)
            count = asset.get_count(date)
            new_count = x_val[i]

            if abs(new_count - count) > eps:
                if new_count > count:
                    tip = 'buy '
                else:
                    tip = 'sell '
                if i in assets_with_real_counts:
                    tip += '%.5f' % abs(new_count - count)
                else:
                    tip += '%d' % round(abs(new_count - count))
            else:
                tip = ''

            distr = '%.2f%%' % (100 * count * price / portfolio_price)
            new_distr = '%.2f%%' %  (100 * new_count * price / target_price)
            delta_count = new_count-count if abs(count-new_count) > 10**-5 else 0
            output.append([asset.name, price, count, price*count, new_count, price*new_count, price*delta_count, tip, distr, new_distr])

        return pd.DataFrame(output, columns=output_cols)
