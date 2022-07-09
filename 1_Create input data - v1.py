import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import datetime
import pymysql
from urllib.parse import quote
import os

## Delete log.txt file
try:
    os.remove("log.txt")
except:
    pass


# query data
today = datetime.date.today()
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
lst_mnth = lastMonth.strftime("%d%b%Y")
lst_beg = lastMonth.strftime("01%b%Y")

disb = "TotalDisbursedAmountAsOn_" + lst_mnth
pos = "Prin_OS_as_on_" + lst_mnth
od_day = "Overdue_Days_as_on_" + lst_mnth

rule_sheet = pd.read_excel('rule_sheet.xlsx')
lendor_list = rule_sheet['Lendor_tag'].values.tolist()

#log.txt
with open('log.txt', 'a') as f:
    f.writelines(f'-------------1. Input file----------------\n')

full_data = []
for i in range(len(lendor_list)):
    lend_name_iter = lendor_list[i]
    # current_cbs_loan_dump
    cnx = create_engine('mysql+pymysql://analytics:%s@34.93.197.76:61306/analytics' % quote('Secure@123'))
    query_1 = f"select KGFS, branch, funder_name, funding_txn_type, funding_txn_remark, URN, AccountNumber, Product, DisbursementDate, SanctionedAmount, {disb}  as disb_amount, Interest_Rate, Installment_Amount, Repayment_Frequency, MaturityDate, {pos} as POS, {od_day} as OD_Days, loan_purpose, loan_purpose_detail, Age, gender, customer_name, father_name, spouse_name, id_proof, id_proof_no, address_proof, address_proof_no, mobile_number, address, district,state, account_status  from perdix_cdr.quick_cbs_loan_dump_{lst_beg}to{lst_mnth} where funding_txn_remark in ('{lend_name_iter}')"
    raw_data = pd.read_sql(query_1, cnx, index_col=None)
    raw_data.to_sql(con=cnx, name='stock_statement_input_data', if_exists='append', index=False)
    cnx.dispose()
    pos_value = raw_data['POS'].sum()
    if pos_value > 0:
        res = f'{i + 1}.{lend_name_iter} - POS value: {pos_value} \n'
        print(res)
    else:
        res = f'{i + 1}.{lend_name_iter} - POS value:{pos_value} Note: May be New Lendor or Lendor_tag name is incorrect in rule sheet \n'
        print(res)

    # notepad Error:
    with open('log.txt', 'a') as f:
        f.writelines(f'{res}')

cnx = create_engine('mysql+pymysql://analytics:%s@34.93.197.76:61306/analytics' % quote('Secure@123'))
query_2 = f"select * from analytics.stock_statement_input_data"
full_data = pd.read_sql(query_2, cnx, index_col=None)
cnx.dispose()

#Product Correction (Home loan improvement)
full_data['Product'].mask(full_data['loan_purpose'] == 'Loans for safe water and sanitation', 'Home Improvement Loan', inplace=True)
full_data['Product'].mask(full_data['loan_purpose'] == 'Construction/purchase/repair House', 'Home Improvement Loan', inplace=True)


full_data.to_excel("raw_data.xlsx", index=False)
print("raw_data.xlsx_created")

# Log.txt:
with open('log.txt', 'a') as f:
    f.writelines(f'raw_data.xlsx file is created\n')


#fresh Unencumbered_data
cnx = create_engine('mysql+pymysql://analytics:%s@34.93.197.76:61306/analytics' % quote('Secure@123'))
query_2 = f"select KGFS, branch, funder_name, funding_txn_type, funding_txn_remark, URN, AccountNumber, Product, DisbursementDate, SanctionedAmount, {disb} as disb_amount, Interest_Rate, Installment_Amount, Repayment_Frequency, MaturityDate, {pos} as POS, {od_day} as OD_Days, loan_purpose, loan_purpose_detail, Age, gender, customer_name, father_name, spouse_name, id_proof, id_proof_no, address_proof, address_proof_no, mobile_number, address, district,state, account_status  from perdix_cdr.quick_cbs_loan_dump_{lst_beg}to{lst_mnth} where funder_name = 'Unencumbered' and book_entity='DvaraKGFS' and Overdue_Days_as_on_{lst_mnth}<=90 and account_status not in ('Closed - pre-closed', 'Closed - after maturity date', 'Closed - on time', 'Frozen')  and product not in ('OTR Loan', 'OTR2', 'OTR Loan 2')  order by DisbursementDate"
raw_data_1 = pd.read_sql(query_2, cnx, index_col=None)
cnx.dispose()

#Product Correction (Home loan improvement)
raw_data_1['Product'].mask(raw_data_1['loan_purpose'] == 'Loans for safe water and sanitation', 'Home Improvement Loan', inplace=True)
raw_data_1['Product'].mask(raw_data_1['loan_purpose'] == 'Construction/purchase/repair House', 'Home Improvement Loan', inplace=True)


raw_data_1.to_excel("Unencumbered_data.xlsx", index=False)
unen_resl_text = f'Unencumbered_data.xlsx_created successfully\n'
print("Unencumbered_data.xlsx_created")

# Log.txt:  
with open('log.txt', 'a') as f:
    f.writelines(f'{unen_resl_text}')
    f.writelines(f'-------------1. Input file Creation completed----------------\n\n\n')