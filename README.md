# NATO Defined Contribution Pension Scheme monitoring tool
Copyright: Christophe Vandeplas <christophe@vandeplas.com>
License: AGPLv3

> WARNING: This code is in active development. Please get in touch with me if you would like to contribute.

The DCPS website currently does keep track of the value of your holdings over time.
This script or library will scrape the website and transform the information in a usable format.
You can then decide what to do with the data:
- Store locally in a SQLite database
- Store in an Google Spreadsheet (TODO)
- Transform the digitalised data (arrays) into something you like.

## Usage
First create a `keys.py` file:
```python
dcps_url = "https://the_url_of_the_dcps_website/login.jsp"
dcps_id = 01234567
dcps_pwd = "yourpassword"
```
To see the parameters supported by the tool, and thus the different output modules:
```sh
python3 dcps.py --help
```

By default it will only show the overview of your holdings:
```
PREVIOUS YEAR BALANCE
Currency    Operation Code         Total Units    #    NAV date      Price per UNIT  Fund                               Amount
----------  ---------------------  -------------  ---  ----------  ----------------  ---------------------------------  ---------
EUR         Previous Year Balance  1,000.000           21/12/2016             1.000  XXX Global Equity W (EUR)          1,000.00
EUR         Previous Year Balance  500.000             21/12/2016            10.000  YYY Global Stock Index Fund (EUR)  5,000.00

CURRENT YEAR CONTRIBUTIONS
  #  Currency    Operation Code                     Total Amount    Reference Date
---  ----------  ---------------------------------  --------------  ----------------
  1  EUR         Contribution                       1,000.00        31/01/2017
  2  EUR         Contribution                       1,000.00        28/02/2017
  3  EUR         Transfer                           10,000.00       20/03/2017
  4  EUR         Switch Out                         -5,000.00       27/03/2017
  5  EUR         Switch In                          5,000.00        27/03/2017
  6  EUR         Contribution                       1,000.00        31/03/2017
  7  EUR         Additional Voluntary Contribution  100.00          31/03/2017
  8  EUR         Contribution                       1,000.00        30/04/2017
  9  EUR         Additional Voluntary Contribution  100.00          30/04/2017
 10  EUR         Contribution                       1,000.00        31/05/2017
 11  EUR         Additional Voluntary Contribution  100.00          31/05/2017
 12  EUR         Contribution                       1,000.00        30/06/2017
 13  EUR         Additional Voluntary Contribution  100.00          30/06/2017
 14  EUR         Contribution                       1,000.00        31/07/2017
 15  EUR         Additional Voluntary Contribution  100.00          31/07/2017

CURRENT BALANCE
Currency    Operation Code        Total Units    #    NAV date      Price per UNIT  Fund                               Amount
----------  --------------------  -------------  ---  ----------  ----------------  ---------------------------------  ---------
EUR         Current Year Balance  10,000.000          04/08/2017             1.000  XXX Global Equity W (EUR)          10,000.00
EUR         Current Year Balance  1,000.000           04/08/2017            10.000  YYY Global Stock Index Fund (EUR)  10,000.00

```
