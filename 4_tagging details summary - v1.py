from sqlalchemy import create_engine
import pandas as pd
pd.options.mode.chained_assignment = None
from urllib.parse import quote
import datetime

today = datetime.date.today()
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
lst_mnth = lastMonth.strftime("%d%b%Y")



#set1
cnx = create_engine('mysql+pymysql://analytics:%s@34.93.197.76:61306/analytics' % quote('Secure@123'))
Unencumbered_list1_query = f"select accountnumber, 'Unencumbered' as funder_name , '' as remarks, '' as transaction_type, '{lst_mnth}' as transaction_date, '' as tag from  analytics.stock_report_removed_excess_acc where accountnumber not in (select accountnumber from analytics.stock_report_selected_acc_set2);"
Unencumbered_list1 = pd.read_sql(Unencumbered_list1_query, cnx, index_col=None)
cnx.dispose()
Unencumbered_list1['status'] = 'Removed accounts'
print('dump1')
#set2
cnx = create_engine('mysql+pymysql://analytics:%s@34.93.197.76:61306/analytics' % quote('Secure@123'))
Unencumbered_list2_query = f"select accountnumber, 'Unencumbered' as funder_name , '' as remarks, '' as transaction_type, '{lst_mnth}' as transaction_date, '' as tag from analytics.stock_report_removed_acc"
Unencumbered_list2 = pd.read_sql(Unencumbered_list2_query, cnx, index_col=None)
cnx.dispose()
Unencumbered_list2['status'] = 'Removed accounts'
print('dump2')
Unencumbered_list_final = pd.concat([Unencumbered_list1, Unencumbered_list2])

#set 3``
cnx = create_engine('mysql+pymysql://analytics:%s@34.93.197.76:61306/analytics' % quote('Secure@123'))
new_tag_list_query = f"select accountnumber, funder_name, funding_txn_remark as remarks, funding_txn_type as transaction_type, '{lst_mnth}' as transaction_date, tag from analytics.stock_full_selected_final"
new_tag_list = pd.read_sql(new_tag_list_query, cnx, index_col=None)
cnx.dispose()

new_tag_list['remarks'] = new_tag_list['tag']


new_tag_list['status'] = 'selcted accounts'
final_out = pd.concat([Unencumbered_list_final, new_tag_list])

final_out.to_excel("taging_details_summary.xlsx", index_label=False)

with open('log.txt', 'a') as f:
    f.writelines(f'-------------4. Summary file Created----------------\n')