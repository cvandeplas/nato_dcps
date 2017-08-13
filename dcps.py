#!/usr/bin/env python3
'''
NATO DCPS monitoring tool - Defined Contribution Pension Scheme
@author: Christophe Vandeplas <christophe@vandeplas.com>
@copyright: AGPLv3
'''

import requests
from bs4 import BeautifulSoup
import re
from tabulate import tabulate
from keys import dcps_url, dcps_id, dcps_pwd


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


# build a permanent session object, this way we keep all cookies and such
s = requests.Session()

# first login on the main portal
payload = {'id': dcps_id, 'pw': dcps_pwd, 'submit': 'SIGN ON'}
r = s.post(dcps_url, data=payload)

# find the details of the form submit to login on the sub-site
soup = BeautifulSoup(r.text, 'lxml')
i = soup.find('input', attrs={"name": "token-authentication"})
payload = {'token-authentication': i.get('value'), 'ecol': 'Go To My Dcps'}
url = i.parent.get('action')  # load the URL from the form, this way we don't expose it here
r = s.post(url, data=payload)
# now we are on the real site with the numbers

#
# load the MY CONTRIBUTION BALANCE page containing all the juicy details
#
soup = BeautifulSoup(r.text, 'lxml')
tmp_input = soup.find('input', value="MAIN-APP-I-I-IOM")
url = '/'.join(r.url.split('/')[:3]) + tmp_input.parent.get('action')  # load the URL from within the page, this way we don't expose it here
payload = {'f-token': 'MAIN-APP-I-I-IOM',
           'c-token': 'MAIN-APP-I-I-IWP-WEP',
           'a-token': 'null'}
r = s.post(url, payload)

soup = BeautifulSoup(r.text, 'lxml')


# Balance Previous year
tmp_balance_at_tds = soup.find_all('td', limit=2, string=re.compile("Balance at"))
balance_year_table = tmp_balance_at_tds[0].parent.parent
results = table_to_dict_array(balance_year_table)

print()
print("BALANCE PREVIOUS YEAR")
print(tabulate(results, headers='keys'))


# Current Year contributions
tmp_year_details_td = soup.find('td', string=re.compile("Current Year Details"))
year_details_table = tmp_year_details_td.parent.parent
results = table_to_dict_array(year_details_table)

print()
print("CURRENT YEAR CONTRIBUTIONS")
print(tabulate(results, headers='keys'))


# Current Balance
balance_now_table = tmp_balance_at_tds[1].parent.parent
results = table_to_dict_array(balance_now_table)

print()
print("CURRENT BALANCE")
print(tabulate(results, headers='keys'))
