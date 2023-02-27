import pandas as pd
import numpy as np
print('Read file starts')
excel_workbook_name = 'Dataset1.xlsx' #Location of the file should be in the same folder as the python file
dataset = pd.read_excel(excel_workbook_name, sheet_name = 'Dataset')
#dataset.head(5)
print('Read file ends')
print('Processing for Resolved INCs starts')
resolved = dataset.loc[dataset['incident_state'] == 'Resolved']
#resolved.head(10)
resolved.reset_index(drop = True, inplace = True)
#resolved.head(10)
#len(resolved)
resolved['dup_check'] = resolved.duplicated(subset = 'number', keep = 'last')
#resolved.head(5)
index = resolved[resolved['dup_check'] == True].index
#index
resolved.drop(index,inplace = True)
#len(resolved)
#resolved.columns
resolved = resolved.drop(resolved.loc[:,'contact_type':'resolved_by'].columns, axis = 1)
#resolved
resolved = resolved.drop(resolved.loc[:,'reassignment_count':'sys_created_at'].columns, axis = 1)
resolved = resolved.drop(['sys_updated_by','closed_at'], axis = 1)
#resolved
resolved = resolved.drop(['dup_check'], axis = 1)
#resolved.to_excel('Resolved.xlsx', index=0)
resolved = resolved.drop(['active'], axis = 1)
resolved.to_excel('Resolved.xlsx', index=0)
#mouli.loc[mouli['resolved_at' == '?']]
resolved.loc[resolved['resolved_at'] == '?', ['resolved_at']] = resolved['sys_updated_at']
#resolved.groupby('resolved_at').count()
#len(resolved)
#resolved.columns
resolved['resolved_at_dt'] = pd.to_datetime(resolved['resolved_at'])
resolved = resolved.drop(['sys_updated_at'], axis = 1)
print('Processing for resolved INCs ends.')
print('Generating Resolved.xlsx file')
resolved.to_excel('Resolved.xlsx', index=0)
print('Generating Resolved.xlsx file ends')
print('Start of Closed INC Process')
closed = dataset.loc[dataset['incident_state'] == 'Closed']
closed.reset_index(drop = True,inplace = True)
#closed
closed['dup_check'] = closed.duplicated(subset = ['number'], keep = 'last')
#closed
#len(closed.loc[closed['dup_check'] == True])
index = closed[closed['dup_check'] == True].index
#index
closed.drop(index,inplace = True)
#len(closed)
#closed.columns
#resolved.columns
closed.to_excel('Closed.xlsx',index = 0)
#closed.loc[closed['resolved_at'] == '?']
closed.reset_index(drop = True,inplace = True)
#closed
closed.drop('dup_check', axis = 1)
#closed.columns
#closed = closed.drop(['reassignment_count','reopen_count', 'sys_mod_count', 'caller_id', 'opened_by', 'contact_type', 'location', 'category', 'subcategory',
#       'u_symptom', 'cmdb_ci', 'assignment_group', 'assigned_to', 'knowledge',
#       'u_priority_confirmation', 'notify', 'problem_id', 'rfc', 'vendor',
#       'caused_by', 'closed_code'], axis = 1)
#closed
closed_fixed = closed.merge(resolved, on = 'number', how = 'left') 
#closed_fixed
closed_fixed = closed_fixed.drop(['resolved_at_x', 'dup_check', 'incident_state_y'], axis = 1)
#closed_fixed
closed_fixed.rename(columns={'incident_state_x':'incident_state','resolved_at_y':'resolved_at'}, inplace=True)
#closed_fixed.columns
closed_fixed.loc[closed_fixed['sys_created_at']== '?', ['sys_created_at']] = closed_fixed['opened_at']
closed_fixed['opened_at_dt'] = pd.to_datetime(closed_fixed['opened_at'])
closed_fixed['sys_created_at_dt'] = pd.to_datetime(closed_fixed['sys_created_at'])
closed_fixed['sys_updated_at_dt'] = pd.to_datetime(closed_fixed['sys_updated_at']) 
closed_fixed['closed_at_dt'] = pd.to_datetime(closed_fixed['closed_at'])
#len(closed_fixed.loc[closed_fixed['sys_created_at']== '?'])
#closed_fixed.loc[closed_fixed['sys_created_at']== '?', ['sys_created_at']] = closed_fixed['opened_at']
closed_fixed.to_excel('Clean_Data.xlsx', index = False)
closed_fixed['resolution_time'] = closed_fixed['resolved_at_dt']-closed_fixed['opened_at_dt']
#closed_fixed
#closed_fixed.loc[closed_fixed['resolution_time'] < 0]
closed_fixed.loc[closed_fixed['resolved_at_dt'].isnull(), ['resolved_at_dt']] = closed_fixed['closed_at_dt']
closed_fixed = closed_fixed.sort_values(by=['opened_at_dt'])
#mouli = pd.read_excel('Resolved.xlsx')
#closed_fixed = closed_fixed.sort_values(by=['opened_at_dt'])
closed_fixed.reset_index(drop = True, inplace = True)
#closed_fixed
print('Closed INCs Process Ends')
print('Generating Clean_Data File')
closed_fixed.to_excel('Clean_Data.xlsx', index = False)
closed_fixed.to_csv('Clean_Data.csv', index = False)
print('Generating Clean_Data File Ends')
