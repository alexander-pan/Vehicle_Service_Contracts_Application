import pyodbc
import pandas as pd
import sys
import os

home = os.environ['HOME']
sys.path.append(home)
from sunpath_creds.dbcreds import server,database,username,password

cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

######################
#   Path Variables   #
######################
linode = '/Sunpath/apps/'
path = '{0}{1}controls.py'.format(home,linode)

#local = '/Projects/Statusquota/sunpath/application/apps/'
#path = '{0}{1}controls.py'.format(home,local)

#Create SELLERS Dictionary
q = """
select * from dbo.seller_funding_data;
"""
df = pd.read_sql(q,cnxn)

sellers = df.SellerName.unique().tolist()
SELLERS = {}
for seller in sellers:
    key,suffix = seller.split('-')
    SELLERS[str(key.strip())] = str(seller)

#Create TXCODES and SHORTCODES Dictionary
q = """
select distinct SunPathSellerCode,SunPathAccountingCode,SellerName
from dbo.daily_extract as t1
join dbo.seller_funding_data as t2
on t1.PolicyNumber = t2.PolicyNumber
join dbo.seller_info_funding_approved_partners as t3
on t1.sellercode = t3.paylinksellercode;
"""
df = pd.read_sql(q,cnxn)

TXCODES = {}
SHORTCODES = {}
for i,row in df.iterrows():
    TXCODES[str(row.SellerName)] = (str(row.SunPathSellerCode),str(row.SunPathAccountingCode))
    key,suffix = row.SellerName.split('-')
    SHORTCODES[str(row.SunPathAccountingCode)] = str(key.strip())

to_add = {'SPF': 'SUNPATH FUNDING',
          'SP_INS': 'SUNPATH INS',
          'SP_ADM': 'SUNPATH ADM',
          'PAYLINK':'PAYLINK',
          None : 'None'}
for k,v in to_add.iteritems():
    SHORTCODES[k] = v

FUNDERS = {
    'SUNPATH (SPF)': 0,
    'SIMPLICITY': 1
}

with open(path,'wb') as handle:
    handle.write('SELLERS = {\n')
    count = 1
    for k,v in sorted(SELLERS.iteritems()):
        if count == len(SELLERS):
            handle.write("'%s': '%s'\n" % (k,v))
        else:
            handle.write("'%s': '%s',\n" % (k,v))
        count+=1
    handle.write('}\n\n')

    handle.write('FUNDERS = {\n')
    count = 1
    for k,v in sorted(FUNDERS.iteritems()):
        if count == len(FUNDERS):
            handle.write("'%s': %d\n" % (k,v))
        else:
            handle.write("'%s': %d,\n" % (k,v))
        count+=1
    handle.write('}\n\n')

    handle.write('TXCODES = {\n')
    count = 1
    for k,v in sorted(TXCODES.iteritems()):
        if count == len(TXCODES):
            handle.write("'%s': %s\n" % (k,v))
        else:
            handle.write("'%s': %s,\n" % (k,v))
        count+=1
    handle.write('}\n\n')

    handle.write('SHORTCODES = {\n')
    count = 1
    for k,v in sorted(SHORTCODES.iteritems()):
        if count == len(SHORTCODES):
            handle.write("'%s': '%s'\n" % (k,v))
        else:
            handle.write("'%s': '%s',\n" % (k,v))
        count+=1
    handle.write('}\n\n')
