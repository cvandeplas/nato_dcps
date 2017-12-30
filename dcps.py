#!/usr/bin/env python3
'''
NATO DCPS monitoring tool - Defined Contribution Pension Scheme
@author: Christophe Vandeplas <christophe@vandeplas.com>
@copyright: AGPLv3
'''

import argparse
import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime
from tabulate import tabulate
try:
    import keys
except Exception:
    exit("ERROR: keys.py file with dcps_url, dcps_id, dcps_pwd does not exist.")


class DCPS:
    def __init__(self, dcps_url, dcps_id, dcps_pwd, args=None):
        self.dcps_url = dcps_url
        self.dcps_id = dcps_id
        self.dcps_pwd = dcps_pwd
        self.web_sess = None       # web session
        self.sql_conn = None    # SQLite connection
        self.webpage_main = None  # response of main webpage

        self.sqlite3_createdb()

    def html_table_to_dict_array(table):
        headers = [header.get_text().strip() for header in table.find_all("th")]
        results = []
        for row in table.find_all('tr'):
            if row.td and row.td.get('colspan'):
                continue
            if row.th:
                continue
            results_row = {}
            for i, col in enumerate(row.find_all('td')):
                results_row[headers[i]] = col.get_text().strip()
            if len(results_row) > 0:
                results.append(results_row)
        return results

    def sqlite3_createdb(self):
        '''
        Singleton to get access to the database connection
        '''
        if self.sql_conn:
            return self.sql_conn

        self.sql_conn = sqlite3.connect('dcps.sqlite3.db')
        c = self.sql_conn.cursor()
        queries = ['CREATE TABLE contributions (date text, date_unix integer, currency text, opcode text, amount real, UNIQUE(date, currency, opcode, amount) ON CONFLICT REPLACE)',
                   'CREATE TABLE balance_now (date text, date_unix integer, currency text, fund text, amount real, total_units real, price_per_unit real, UNIQUE(date, currency, fund, amount) ON CONFLICT REPLACE)',
                   'CREATE TABLE balance_year (date text, date_unix integer, currency text, fund text, amount real, total_units real, price_per_unit real, UNIQUE(date, currency, fund, amount) ON CONFLICT REPLACE)',
                   'CREATE TABLE contributions_detail(date_operation text, date_operation_unix integer, date_nav text, date_nav_unix integer, fund text, exchange_rate real, amount_gross real, fees real, amount_net real, units real, price_per_unit real, UNIQUE(date_operation, fund, units) ON CONFLICT REPLACE)']
        for query in queries:
            try:
                c.execute(query)
            except sqlite3.OperationalError as e:
                pass
        self.sql_conn.commit()
        return self.sql_conn

    def normalise_data(data):
        for row in data:
            # transform the string numbers into float numbers
            for col in ['Amount', 'Exchange Rate', 'Fees (*)', 'Gross Amount Inv/Dis', 'Net Amount Inv/Dis',
                        'No. of Units', 'Price per Unit', 'Price per UNIT', 'Total Amount', 'Total Units']:
                if col in row:
                    row[col] = float(row[col].replace(',', ''))
        return data

    def date_to_unix(s):
        return time.mktime(datetime.strptime(s, "%d/%m/%Y").timetuple())

    def db_insert_contributions(self, data):
        c = self.sql_conn.cursor()
        for i in data:
            c.execute("INSERT INTO contributions VALUES(?,?,?,?,?)", [
                i['Reference Date'],
                DCPS.date_to_unix(i['Reference Date']),
                i['Currency'],
                i['Operation Code'],
                i['Total Amount']])
        self.sql_conn.commit()

    def db_insert_contributions_detail(self, data):
        c = self.sql_conn.cursor()
        for i in data:
            if len(i) == 0:
                continue
            c.execute("INSERT INTO contributions_detail VALUES(?,?,?,?,?,?,?,?,?,?,?)", [
                i['Operation Date'],
                DCPS.date_to_unix(i['Operation Date']),
                i['Nav Date'],
                DCPS.date_to_unix(i['Nav Date']),
                i['Fund'],
                i['Exchange Rate'],
                i['Gross Amount Inv/Dis'],
                i['Fees (*)'],
                i['Net Amount Inv/Dis'],
                i['No. of Units'],
                i['Price per Unit']])
        self.sql_conn.commit()

    def db_insert_balance_now(self, data):
        c = self.sql_conn.cursor()
        for i in data:
            c.execute("INSERT INTO balance_now VALUES(?,?,?,?,?,?,?)", [
                i['NAV date'],
                DCPS.date_to_unix(i['NAV date']),
                i['Currency'],
                i['Fund'],
                i['Amount'],
                i['Total Units'],
                i['Price per UNIT']])
        self.sql_conn.commit()

    def db_insert_balance_year(self, data):
        c = self.sql_conn.cursor()
        for i in data:
            c.execute("INSERT INTO balance_year VALUES(?,?,?,?,?,?,?)", [
                i['NAV date'],
                DCPS.date_to_unix(i['NAV date']),
                i['Currency'],
                i['Fund'],
                i['Amount'],
                i['Total Units'],
                i['Price per UNIT']])
        self.sql_conn.commit()

    def web_login(self):
        if self.webpage_main:   # we are already authenticated, no new login needed
            return self.webpage_main
        # build a permanent session object, this way we keep all cookies and such
        self.web_sess = requests.Session()

        # first login on the main portal
        payload = {'id': self.dcps_id, 'pw': self.dcps_pwd, 'submit': 'SIGN ON'}
        r = self.web_sess.post(self.dcps_url, data=payload)

        # find the details of the form submit to login on the sub-site
        soup = BeautifulSoup(r.text, 'lxml')
        i = soup.find('input', attrs={"name": "token-authentication"})
        if not i:
            exit("ERROR: Authentication error, cannot login.")
        payload = {'token-authentication': i.get('value'), 'ecol': 'Go To My Dcps'}
        url = i.parent.get('action')  # load the URL from the form, this way we don't expose it here
        r = self.web_sess.post(url, data=payload)
        if "Your TEMPORARY first-access password" in r.text:
            exit("ERROR: Change of password requested. Please login manually and change your password.")
        # now we are on the real site with the numbers
        self.webpage_main = r
        return self.webpage_main

    def db_update_from_webpage(self):
        self.web_login()

        # load the MY CONTRIBUTION BALANCE page containing all the juicy details
        soup = BeautifulSoup(self.webpage_main.text, 'lxml')
        tmp_input = soup.find('input', value="MAIN-APP-I-I-IOM")
        url = '/'.join(self.webpage_main.url.split('/')[:3]) + tmp_input.parent.get('action')  # load the URL from within the page, this way we don't expose it here
        payload = {'f-token': 'MAIN-APP-I-I-IOM',
                   'c-token': 'MAIN-APP-I-I-IWP-WEP',
                   'a-token': 'null'}
        r = self.web_sess.post(url, payload)
        soup = BeautifulSoup(r.text, 'lxml')

        # Balance Previous year
        tmp_balance_at_tds = soup.find_all('td', limit=2, string=re.compile("Balance at"))
        balance_year_table = tmp_balance_at_tds[0].parent.parent
        results = DCPS.normalise_data(DCPS.html_table_to_dict_array(balance_year_table))
        print()
        print("BALANCE PREVIOUS YEAR")
        print(tabulate(results, headers='keys'))
        self.db_insert_balance_year(results)

        # Current Year contributions - Summary
        tmp_year_details_td = soup.find('td', string=re.compile("Current Year Details"))
        year_details_table = tmp_year_details_td.parent.parent
        results = DCPS.normalise_data(DCPS.html_table_to_dict_array(year_details_table))
        print()
        print("CURRENT YEAR CONTRIBUTIONS - SUMMARY")
        print(tabulate(results, headers='keys'))
        self.db_insert_contributions(results)

        # Current Year contributions - Detail
        print()
        print("CURRENT YEAR CONTRIBUTIONS - DETAILS")
        ahrefs = year_details_table.find_all('a')
        urls = set()
        contributions_detail = []
        for ahref in ahrefs:
            urls.add(ahref.get('href'))
        j = 0
        for url in urls:
            url = '/'.join(r.url.split('/')[:3]) + url  # load the URL from within the page, this way we don't expose it here
            url_r = self.web_sess.get(url)
            j += 1
            url_soup = BeautifulSoup(url_r.text, 'lxml')
            tmp_operation_date_td = url_soup.find('th', string=re.compile("Operation Date"))
            url_details_table = tmp_operation_date_td.find_parent('table')
            url_results = DCPS.normalise_data(DCPS.html_table_to_dict_array(url_details_table))
            contributions_detail += url_results
            self.db_insert_contributions_detail(url_results)
        print()
        print(tabulate(contributions_detail, headers='keys'))

        # Current Balance
        balance_now_table = tmp_balance_at_tds[1].parent.parent
        results = DCPS.normalise_data(DCPS.html_table_to_dict_array(balance_now_table))
        print()
        print("CURRENT BALANCE")
        print(tabulate(results, headers='keys'))
        self.db_insert_balance_now(results)

    def db_update_from_pdf(self, fname):
        '''
        FIXME WORK IN PROGRESS
        FIXME WORK IN PROGRESS
        FIXME WORK IN PROGRESS


        This is ugly code that will break when the PDF format changes.
        However it seems to do the trick.
        It will extract the text from the pdf using the slate library,
        and then transform the blob into nice tables,
        and will finally store everything into the database

        You are free to email me your PDF files if they break and I will try to fix it.
        I will not keep any file you send me.
        '''
        import slate
        # TODO verify slate version
        print("Reading PDF file {}".format(fname))
        with open(fname, 'rb') as f:
            doc = slate.PDF(f)
        contributions_detail = []

        for page in doc:
            if 'Holdings (SUMMARY)' in page:
                print("Holdings (SUMMARY) page")
            elif 'Holdings (DETAIL)' in page:
                # remove header line and other pollution
                page = re.sub(r'^.*Holdings \(DETAIL\)', '', page)
                page = re.sub(r'TransactionInvestment', '', page)
                page = re.sub(r'\x0c', '', page)
                page = re.sub(r'TOTAL([0-9]{2}/[0-9]{2}/[0-9]{4})([A-Z]{3}[a-zA-Z ]+\([A-Z]{3}\))(-?[0-9.]+,[0-9]{3})', '', page)
                # extract the columns with a big regex - note the last 2 columns are presented in a different way than they are visually seen on the page.
                #   ([a-zA-Z \(\)\*]+)                  # operation code
                #   ([0-9]{2}/[0-9]{2}/[0-9]{4})        # date
                #   ([A-Z]{3}[a-zA-Z ]+\([A-Z]{3}\))    # fund (CURR)
                #   (-?[0-9.]+,[0-9]{2})                # total amount
                #   ([A-Z]{3})                          # currency
                #   (-?[0-9.]+,[0-9]{2})                # amount invested
                #   (-?[0-9.]+,[0-9]{3})                # total units
                #   ([0-9.]+,[0-9]{4})                  # price per unit
                page = re.sub(
                    r'([a-zA-Z \(\)\*]+)([0-9]{2}/[0-9]{2}/[0-9]{4})([A-Z]{3}[a-zA-Z ]+\([A-Z]{3}\))(-?[0-9.]+,[0-9]{2})([A-Z]{3})(-?[0-9.]+,[0-9]{2})(-?[0-9.]+,[0-9]{3})([0-9.]+,[0-9]{4})',
                    r'\1\t\2\t\3\t\4\t\5\t\6\t\8\t\7\n',
                    page)
                page = page.strip('\n')         # cleanup last newline our regex added
                # we now have a clean table, iterate over each row in our newly made table and put everything in something usable
                for line in page.split('\n'):
                    # remove remaining polluting lines: summary, header
                    if line.startswith('TOTAL') or line == 'Operation CodeFundInvestmentTotal AmountCurrencyTransactionAmount Invested /Disinvested / AccruedPrice per Unit(NAV)ReferenceDateTotal Units':
                        continue
                    line = line.replace('.', '').replace(',', '.')  # fix number format to match the website's format
                    items = re.split('\t', line)
                    contributions_detail.append(items)

        contributions_detail = DCPS.normalise_data(DCPS.pdf_contributions_detail_list_to_dict_array(contributions_detail))
        self.db_insert_contributions_detail(contributions_detail)
        # do something with the extracted data
        if len(contributions_detail):
            print("Holdings (DETAIL) page - contributions")
            print(tabulate(contributions_detail, headers='keys'))

            print()
            print("Computed contributions (summary)")
            contributions = DCPS.pdf_contributions_detail_dict_to_contributions_dict(contributions_detail)
            self.db_insert_contributions(contributions)
            print(tabulate(contributions, headers='keys'))

    def db_get_funds(self):
        '''
        returns a list of the funds
        '''
        c = self.sql_conn.cursor()
        c.execute("SELECT DISTINCT fund FROM balance_now ORDER BY fund")
        result = c.fetchall()
        return [x[0] for x in result]

    # JUST PLAYING AROUND
    def db_get_contributions_sum(self):
        c = self.sql_conn.cursor()
        c.execute("SELECT SUM(amount) FROM contributions")
        result = c.fetchall()
        return round(result[0][0], 2)

    def db_get_latest_balance(self):
        '''
        get the sum of the funds at the last balance
        '''
        c = self.sql_conn.cursor()
        c.execute("SELECT amount FROM balance_now ORDER BY date_unix DESC LIMIT (SELECT COUNT (DISTINCT fund) FROM balance_now)")
        result = c.fetchall()
        return sum([x[0] for x in result])
        # return round(result[0][0], 2)

    def pdf_contributions_detail_list_to_dict_array(table):
        '''
        build a compatible list to insert the contributions as contributions_detail into the database
        '''
        results = []
        for row in table:
            results.append({
                'Operation Code': row[0],
                'Operation Date': row[1],
                'Nav Date': row[1],        # this is not really correct, but it's the best we can do now, and it should not affect the rest as the unique constraint is not on the nav_date
                'Fund': row[2],
                'Total Amount': row[3],
                'Currency': row[4],
                'Gross Amount Inv/Dis': row[5],
                'Net Amount Inv/Dis': row[5],
                'Price per Unit': row[6],
                'No. of Units': row[7],
                'Exchange Rate': '1.0',
                'Fees (*)': '0.0'
            })
        return results

    def pdf_contributions_detail_dict_to_contributions_dict(table):
        '''
        build a summary of contributions using contribution details extracted from the PDF
        '''
        # LATER we could do this easily with numpy, but it's yet one more library to import, so we'll do it manually this time
        # We need to keep
        #   i['Reference Date'],
        #   i['Currency'],
        #   i['Operation Code'],
        #   i['Total Amount']
        # for each 'Operation Date', and 'Operation Code' group the items together by taking the sum
        tmp_computation = {}
        for row in table:
            if not tmp_computation.get(row['Operation Date']):
                tmp_computation[row['Operation Date']] = {}
            if not tmp_computation[row['Operation Date']].get(row['Operation Code']):
                tmp_computation[row['Operation Date']][row['Operation Code']] = {}
            if not tmp_computation[row['Operation Date']][row['Operation Code']].get('Total Amount'):
                tmp_computation[row['Operation Date']][row['Operation Code']]['Total Amount'] = 0
            tmp_computation[row['Operation Date']][row['Operation Code']]['Currency'] = row['Currency']
            tmp_computation[row['Operation Date']][row['Operation Code']]['Total Amount'] += row['Total Amount']
        # then rebuild a proper array with dicts
        results = []
        for op_date, v1 in tmp_computation.items():
            for op_code, v2 in v1.items():
                # print("{} {:15} \t{}".format(op_date, op_code, v2))
                results.append({
                    'Reference Date': op_date,
                    'Currency': v2['Currency'],
                    'Operation Code': op_code,
                    'Total Amount': round(v2['Total Amount'], 2)
                })
        return results


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(description='Monitor and report your NATO Defined Contribution Pension Scheme holdings.')
    # parser.add_argument('-q', '--quiet', dest='quiet', action='store_true', help='output nothing except errors. great for cronjobs (TODO)')  # TODO implement this
    # parser.add_argument('--pdf', dest='pdf', action='store_true', help='process historical Individual Statement PDFs')
    # parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='extra verbosity')

    # --first-run or --magic -- first run, do magic: extract data, extract old data from Individual Statement PDFs and compute data based on historical fund value
    # -f -- path for database, (default dcps.sqlite3.db)

    dcps = DCPS(keys.dcps_url, keys.dcps_id, keys.dcps_pwd)
    dcps.db_update_from_webpage()

    # TESTING - WORK IN PROGRESS
    #
    # print(dcps.db_get_funds())

    # print(dcps.db_get_contributions_sum())

    # print(dcps.db_get_latest_balance())

    # fnames = ['docs/filename_1514390050721.pdf', 'docs/filename_1514390077314.pdf']
    # for fname in fnames:
    #     dcps.db_update_from_pdf(fname)
