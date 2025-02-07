# Python web scraper for CHP data

# the same directory from which the code is run needs to contain:
# - combined_table_PowerBI.csv
# - data.pdf (this just has to be a pdf file. It is needed as a container,
# otherwise the code will not be able to download pdf

import pdfplumber
import pandas as pd
import numpy as np
import requests
import os
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from datetime import datetime
import re
#import matplotlib.pyplot as plt

os.chdir(r"C:\Users\Chrissy Pang\Desktop\test")

def extract(month, year, filename=None, trace=False, minpage=1, maxpage=1):
    # extractor for tables in the pdf documents
    # month = month in range(0, 13)
    # year = year
    # filename - overrides month + year if a particular filename is used
    # trace - whether to print output for debugging
    # minpage, maxpage - first and last page where the tables are
    
    if filename is None:
        url = 'https://www.chp.gov.hk/files/pdf/ipd_' + str(year) + ('0' if month < 10 else '') + str(month) + '.pdf'
    else:
        url = filename
    print(url)
    
    r = requests.get(url)
    if "Sorry, the page you requested cannot be found." in str(r.content):
        url = 'https://www.chp.gov.hk/files/pdf/ipd_' + str(year) + ('0' if month < 10 else '') + str(month) + '_tagged.pdf'
        print(url)
        r = requests.get(url)
          
    # write data to local file, else you can't scrape it
    open('data.pdf', 'wb').write(r.content)
    pdf = pdfplumber.open("data.pdf")
    
    textlist = []
    for pagenum in range(minpage, maxpage+1):
        page = pdf.pages[pagenum]
        textlist += page.extract_text().split("\n") # this splitting sometimes removes row names
        
    if trace:
        print(textlist)
    
    # attempt to remove annoying characters
    for i in range(len(textlist)):
        for char in ['|', ' ‡', '‡', '§', '*', '†']:
            textlist[i] = textlist[i].replace(char, '')
    
    # here we could find the rows with exactly 8 numbers, regardless of what else is in the row
    y = [x for x in textlist if (len(re.findall(r'\d+', x)) == 7 or len(x.split(' ')) == 8)]
    y = [x for x in y if len(re.findall(r'\d+', x)) > 0]
    # this doesn't work because Serotype is a number
    if trace:
        print(y)
    
    totalrows = np.where(['Total' in x for x in y]) # rows where a table ends
    table1 = y[:(int(totalrows[0][0])+1)]
    table2 = y[(int(totalrows[0][0])+1):]
    
    out = {}
    for table in [1, 2]:
        if trace:
            print(table)
        #tab = pd.DataFrame([[''.join(re.findall('[a-zA-Z]', x))] + re.findall('\d+', x) for x in eval('table' + str(table))])
        tab = []
        for x in eval('table' + str(table)):
            numbers = x.split(" ")[-7:]
            row_header = ''.join(x.split(" ")[:-7])
            tab += [[row_header] + numbers]
        tab = pd.DataFrame(tab)
        
        if trace:
            print(tab)
            print(tab.shape)
            
        if tab.shape[0] > 0:
        # for the first few years, there is only one table
            
            tab.columns = ['', '<2', '2-4', '5-17', '18-49', '50-64', '65+', 'Total']
            tab.index = tab.iloc[:,0]
            tab = tab.iloc[:, 1:]

            # here we need to remove all annotations from the numbers in the table
            for i in range(tab.shape[0]):
                for j in range(tab.shape[1]):
                    u = tab.iloc[i,j]
                    unum = re.findall(r'\d+', u)
                    if len(unum) > 0:
                        u = unum[0]
                    else:
                        u = 0
                    tab.iloc[i,j] = u

            out['Table ' + str(table)] = tab
            
            if (tab.shape[0] <=1) and (tab.astype('float').values.sum() > 0):
                print("Error. Not enough rows.")

            if (tab.iloc[:-1, :].astype('float').sum(axis=0).values - tab.loc['Total',:].astype('float').values).sum() > 1e-6:
                print(tab.iloc[:-1, :].astype('float').sum(axis=0))
                print(tab.loc['Total',:].astype('float'))
                print("Error. Row total does not match.")

            if '' in tab.index:
                print("Error. Missing row name.")
        
    return out
    
def process_scraped_data(tab, year, month, table_name = "Table 2"):
    # make a scraped table into a form in which it can be combined with the existing data
    out = tab[table_name].reset_index()
    if out.shape[0] > 0:
        out.columns = ["Serotype", "< 2", "2--4", "5--17", "18--49", "50--64", "65+", "Total"]
        out["Year"] = year
        out["Month"] = month
        out['date'] = pd.to_datetime(dict(year=out['Year'], month=out['Month'], day=1)).dt.strftime('%#d/%#m/%Y')
    else: 
        return tab
    return out
    
# fetch existing data
data = pd.read_excel("combined_table_PowerBI.xlsx").iloc[:, 1:] # remove index column
current_rows = data.shape[0]

current_time = pd.to_datetime(datetime.now())
current_year = current_time.year
current_month = current_time.month

max_existing_date = pd.to_datetime(data['date']).max()

if (current_time - max_existing_date).days >= 60:
    if current_month <= 3:
        year = current_year - 1
        month = current_month - 3 + 12
    else:
        month = current_month - 3
        year = current_year
    
    if ((data['Year'] == year) & (data['Month'] == month)).sum() <= 0:
        try:
            m3 = process_scraped_data(extract(month, year), year, month)
        except:
            print("Scraping for " + str(year) + "-" + str(month) + " failed")
            m3 = pd.DataFrame()
    else:
        m3 = pd.DataFrame()
    if current_month <= 2:
        year = current_year - 1
        month = current_month - 2 + 12
    else:
        month = current_month - 2
        year = current_year
    
    if ((data['Year'] == year) & (data['Month'] == month)).sum() <= 0:
        try:
            m2 = process_scraped_data(extract(month, year), year, month)
        except:
            print("Scraping for " + str(year) + "-" + str(month) + " failed")
            m2 = pd.DataFrame()
    else:
        m2 = pd.DataFrame()

    if current_month <= 1:
        year = current_year - 1
        month = current_month - 1 + 12
    else:
        month = current_month - 1
        year = current_year

    if ((data['Year'] == year) & (data['Month'] == month)).sum() <= 0:
        try:
            m1 = process_scraped_data(extract(month, year), year, month)
        except:
            print("Scraping for " + str(year) + "-" + str(month) + " failed")
            m1 = pd.DataFrame()
    else:
        m1 = pd.DataFrame()

data = pd.concat([data, m3])        
data = pd.concat([data, m2])
data = pd.concat([data, m1])

print(data.columns)
print(m2.columns)

for col in ["< 2", "2--4", "5--17", "18--49", "50--64", "65+", "Total"]:
    data[col] = data[col].astype(int)
    
#data['date'] = pd.to_datetime(data['date']).dt.strftime('%#d/%#m/%Y')

if data.shape[0] > current_rows:
    data.reset_index().iloc[:, 1:].to_excel("combined_table_PowerBI.xlsx", sheet_name="Sheet1") # might need changing later

# we also need to make the monthly pattern out of the combined_table_powerbi table
seasonal = data.copy()
seasonal = seasonal.loc[seasonal["Serotype"] == "Total", :]
seasonal = seasonal.loc[:, ["Total", "Year", "Month", "date"]]

fy = seasonal["Year"].copy()
fy[seasonal["Month"] <= 6] = fy[seasonal["Month"] <= 6] - 1
seasonal["FISCAL_YEAR"] = fy

fm = seasonal["Month"].copy()
fm[seasonal["Month"] <= 6] = fm[seasonal["Month"] <= 6] + 6
fm[seasonal["Month"] > 6] = fm[seasonal["Month"] > 6] - 6
seasonal["FISCAL_MONTH"] = fm

seasonal["Year_new"] = [str(x) + '-' + str(x+1) for x in seasonal["FISCAL_YEAR"]]

seasonal = seasonal.loc[seasonal["FISCAL_YEAR"] >= 2015, ].reset_index(drop=True)

if data.shape[0] > current_rows:
    seasonal.to_excel("seasonal_pattern.xlsx", sheet_name="Sheet1")
    
