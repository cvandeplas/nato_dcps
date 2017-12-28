#!/usr/bin/env python3
'''
NATO DCPS monitoring tool - Defined Contribution Pension Scheme
@author: Christophe Vandeplas <christophe@vandeplas.com>
@copyright: AGPLv3
'''

import requests
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime
from tabulate import tabulate
try:
    from keys import dcps_url, dcps_id, dcps_pwd
except Exception:
    exit("ERROR: keys.py file with dcps_url, dcps_id, dcps_pwd does not exist.")


def table_to_dict_array(table):
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
        results.append(results_row)
    return results


def sqlite3_createdb():
    import sqlite3
    conn = sqlite3.connect('dcps.sqlite3.db')
    c = conn.cursor()
    queries = ['CREATE TABLE contributions (date text, date_unix integer, currency text, opcode text, amount real, UNIQUE(date, currency, opcode, amount) ON CONFLICT REPLACE)',
               'CREATE TABLE balance_now (date text, date_unix integer, currency text, fund text, amount real, total_units real, price_per_unit real, UNIQUE(date, currency, fund, amount) ON CONFLICT REPLACE)',
               'CREATE TABLE balance_year (date text, date_unix integer, currency text, fund text, amount real, total_units real, price_per_unit real, UNIQUE(date, currency, fund, amount) ON CONFLICT REPLACE)',
               'CREATE TABLE contributions_detail(date_operation text, date_operation_unix integer, date_nav text, date_nav_unix integer, fund text, exchange_rate real, amount_gross real, fees real, amount_net real, units real, price_per_unit real, UNIQUE(date_operation, date_nav, fund, units) ON CONFLICT REPLACE)']
    for query in queries:
        try:
            c.execute(query)
        except sqlite3.OperationalError as e:
            pass
    conn.commit()
    return conn


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


def db_update_from_online():
    # build a permanent session object, this way we keep all cookies and such
    s = requests.Session()

    # first login on the main portal
    payload = {'id': dcps_id, 'pw': dcps_pwd, 'submit': 'SIGN ON'}
    r = s.post(dcps_url, data=payload)

    # find the details of the form submit to login on the sub-site
    soup = BeautifulSoup(r.text, 'lxml')
    i = soup.find('input', attrs={"name": "token-authentication"})
    if not i:
        exit("ERROR: Authentication error, cannot login.")
    payload = {'token-authentication': i.get('value'), 'ecol': 'Go To My Dcps'}
    url = i.parent.get('action')  # load the URL from the form, this way we don't expose it here
    r = s.post(url, data=payload)
    # now we are on the real site with the numbers

    #
    # load the MY CONTRIBUTION BALANCE page containing all the juicy details
    #
    if "Your TEMPORARY first-access password" in r.text:
        exit("ERROR: Change of password requested. Please login manually and change your password.")
    soup = BeautifulSoup(r.text, 'lxml')
    tmp_input = soup.find('input', value="MAIN-APP-I-I-IOM")
    url = '/'.join(r.url.split('/')[:3]) + tmp_input.parent.get('action')  # load the URL from within the page, this way we don't expose it here
    payload = {'f-token': 'MAIN-APP-I-I-IOM',
               'c-token': 'MAIN-APP-I-I-IWP-WEP',
               'a-token': 'null'}
    r = s.post(url, payload)
    soup = BeautifulSoup(r.text, 'lxml')

    sql_conn = sqlite3_createdb()

    # Balance Previous year
    tmp_balance_at_tds = soup.find_all('td', limit=2, string=re.compile("Balance at"))
    balance_year_table = tmp_balance_at_tds[0].parent.parent
    results = normalise_data(table_to_dict_array(balance_year_table))
    print()
    print("BALANCE PREVIOUS YEAR")
    print(tabulate(results, headers='keys'))
    c = sql_conn.cursor()
    for i in results:
        c.execute("INSERT INTO balance_year VALUES(?,?,?,?,?,?,?)", [
            i['NAV date'],
            date_to_unix(i['NAV date']),
            i['Currency'],
            i['Fund'],
            i['Amount'],
            i['Total Units'],
            i['Price per UNIT']])
    sql_conn.commit()

    # Current Year contributions - Summary
    tmp_year_details_td = soup.find('td', string=re.compile("Current Year Details"))
    year_details_table = tmp_year_details_td.parent.parent
    results = normalise_data(table_to_dict_array(year_details_table))
    print()
    print("CURRENT YEAR CONTRIBUTIONS - SUMMARY")
    print(tabulate(results, headers='keys'))
    c = sql_conn.cursor()
    for i in results:
        c.execute("INSERT INTO contributions VALUES(?,?,?,?,?)", [
            i['Reference Date'],
            date_to_unix(i['Reference Date']),
            i['Currency'],
            i['Operation Code'],
            i['Total Amount']])
    sql_conn.commit()

    # Current Year contributions - Detail
    print()
    print("CURRENT YEAR CONTRIBUTIONS - DETAILS")
    ahrefs = year_details_table.find_all('a')
    urls = set()
    for ahref in ahrefs:
        urls.add(ahref.get('href'))
    j = 0
    for url in urls:
        url = '/'.join(r.url.split('/')[:3]) + url  # load the URL from within the page, this way we don't expose it here
        url_r = s.get(url)
        j += 1
        url_soup = BeautifulSoup(url_r.text, 'lxml')
        tmp_operation_date_td = url_soup.find('th', string=re.compile("Operation Date"))
        url_details_table = tmp_operation_date_td.find_parent('table')
        url_results = normalise_data(table_to_dict_array(url_details_table))
        print()
        print(tabulate(url_results, headers='keys'))
        c = sql_conn.cursor()
        for i in url_results:
            if len(i) == 0:
                continue
            c.execute("INSERT INTO contributions_detail VALUES(?,?,?,?,?,?,?,?,?,?,?)", [
                i['Operation Date'],
                date_to_unix(i['Operation Date']),
                i['Nav Date'],
                date_to_unix(i['Nav Date']),
                i['Fund'],
                i['Exchange Rate'],
                i['Gross Amount Inv/Dis'],
                i['Fees (*)'],
                i['Net Amount Inv/Dis'],
                i['No. of Units'],
                i['Price per Unit']])
        sql_conn.commit()

    # Current Balance
    balance_now_table = tmp_balance_at_tds[1].parent.parent
    results = normalise_data(table_to_dict_array(balance_now_table))
    print()
    print("CURRENT BALANCE")
    print(tabulate(results, headers='keys'))
    c = sql_conn.cursor()
    for i in results:
        c.execute("INSERT INTO balance_now VALUES(?,?,?,?,?,?,?)", [
            i['NAV date'],
            date_to_unix(i['NAV date']),
            i['Currency'],
            i['Fund'],
            i['Amount'],
            i['Total Units'],
            i['Price per UNIT']])
    sql_conn.commit()


def db_get_funds():
    '''
    returns a list of the funds
    '''
    sql_conn = sqlite3_createdb()
    c = sql_conn.cursor()
    c.execute("SELECT DISTINCT fund FROM balance_now ORDER BY fund")
    result = c.fetchall()
    return [x[0] for x in result]


# JUST PLAYING AROUND
def db_get_contributions_sum():
    sql_conn = sqlite3_createdb()
    c = sql_conn.cursor()
    c.execute("SELECT SUM(amount) FROM contributions")
    result = c.fetchall()
    return round(result[0][0], 2)


def db_get_latest_balance():
    '''
    get the sum of the funds at the last balance
    '''
    sql_conn = sqlite3_createdb()
    c = sql_conn.cursor()
    c.execute("SELECT amount FROM balance_now ORDER BY date_unix DESC LIMIT (SELECT COUNT (DISTINCT fund) FROM balance_now)")
    result = c.fetchall()
    return sum([x[0] for x in result])
    # return round(result[0][0], 2)


# db_update_from_online()

# FIXME add command line parsing and help
# -q -- quiet mode, output nothing except errors. great as cronjob
# -d <> -- debug 1 to 9
# --first-run or --magic -- first run, do magic: extract data, extract old data from Individual Statement PDFs and compute data based on historical fund value
# -f -- path for database, (default dcps.sqlite3.db)



# TESTING
# sql_conn = sqlite3_createdb()
# c = sql_conn.cursor()

# c.execute("SELECT * FROM contributions_detail ORDER BY date_nav_unix DESC")
# print(c.fetchone())

# print(db_get_funds())

# print(db_get_contributions_sum())

# print(db_get_latest_balance())

