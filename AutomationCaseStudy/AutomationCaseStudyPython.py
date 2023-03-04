
import pandas as pd
#print('Read file starts')
#READING EXCEL FILE STARTS...!
excel_workbook_name = 'ExcelDataset.xlsx'  #Name of the excel file should be same as the Excel file createed in UI Path
excel_sheet_name = 'Dataset' #Name of the sheet should be same as that given in UI Path

#Output files for futher processing
clean_data_file_Name = 'Clean_Data.xlsx'
sla_breached_file_name = 'SLA_Breached_File.xlsx'
sla_met_file_name = 'SLA_Met.xlsx'
top_five_sla_miss = 'top_five_SLA_miss.xlsx'
top_five_sla_met = 'top_five_SLA_met.xlsx'

dataset = pd.read_excel(excel_workbook_name, excel_sheet_name)

#File contains all the updates made to the Incidents such as Assignment, 
#updating status as pending, updating as resolved and finally closed.
#Each of these are given as a seperate row in the data sheet.
#We should filter only the resovled and closed data for the case study
#Processing for Resovled incidents                        
#print('Processing for Resolved INCs starts')
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
#resolved.to_excel('Resolved.xlsx', index=0)
#mouli.loc[mouli['resolved_at' == '?']]
resolved.loc[resolved['resolved_at'] == '?', ['resolved_at']] = resolved['sys_updated_at']
#resolved.groupby('resolved_at').count()
#len(resolved)
#resolved.columns
resolved['resolved_at_dt'] = pd.to_datetime(resolved['resolved_at'])
resolved = resolved.drop(['sys_updated_at'], axis = 1)
#print('Processing for resolved INCs ends.')
#print('Generating Resolved.xlsx file')
#resolved.to_excel('Resolved.xlsx', index=0)

#print('Start of Closed INC Process')
closed = dataset.loc[dataset['incident_state'] == 'Closed']
closed.reset_index(drop = True,inplace = True)
closed['dup_check'] = closed.duplicated(subset = ['number'], keep = 'last')
index = closed[closed['dup_check'] == True].index
closed.drop(index,inplace = True)
closed.reset_index(drop = True,inplace = True)
closed.drop('dup_check', axis = 1)
closed_fixed = closed.merge(resolved, on = 'number', how = 'left') 
closed_fixed = closed_fixed.drop(['resolved_at_x', 'dup_check', 'incident_state_y'], axis = 1)
closed_fixed.rename(columns={'incident_state_x':'incident_state','resolved_at_y':'resolved_at'}, inplace=True)
closed_fixed.loc[closed_fixed['sys_created_at']== '?', ['sys_created_at']] = closed_fixed['opened_at']
closed_fixed['opened_at_dt'] = pd.to_datetime(closed_fixed['opened_at'])
closed_fixed['sys_created_at_dt'] = pd.to_datetime(closed_fixed['sys_created_at'])
closed_fixed['sys_updated_at_dt'] = pd.to_datetime(closed_fixed['sys_updated_at']) 
closed_fixed['closed_at_dt'] = pd.to_datetime(closed_fixed['closed_at'])
closed_fixed.loc[closed_fixed['resolved_at_dt'].isnull(), ['resolved_at_dt']] = closed_fixed['closed_at_dt']
closed_fixed['resolution_time'] = (closed_fixed['resolved_at_dt']-closed_fixed['opened_at_dt']).dt.days
closed_fixed.loc[closed_fixed['resolution_time'] < 0, ['resolution_time']] = -1*closed_fixed['resolution_time']
closed_fixed = closed_fixed.sort_values(by=['opened_at_dt'])
closed_fixed.reset_index(drop = True, inplace = True)
closed_fixed['SLA_REF'] = ''
closed_fixed['BREACH'] = ''
closed_fixed.loc[closed_fixed['priority'] == '3 - Moderate', ['SLA_REF']] = 20
closed_fixed.loc[closed_fixed['priority'] == '2 - High', ['SLA_REF']] = 10
closed_fixed.loc[closed_fixed['priority'] == '4 - Low', ['SLA_REF']] = 35
closed_fixed.loc[closed_fixed['priority'] == '1 - Critical', ['SLA_REF']] = 5
closed_fixed.loc[closed_fixed['SLA_REF'] >= closed_fixed['resolution_time'], ['BREACH']] = 'NO'
closed_fixed.loc[closed_fixed['SLA_REF'] < closed_fixed['resolution_time'], ['BREACH']] = 'YES'
closed_fixed['Assignee_Email'] = closed_fixed['resolved_by']+'@email.com'
#closed_fixed
#print('Closed INCs Process Ends')
#print('Generating Clean_Data File')
closed_fixed.to_excel('Clean_Data.xlsx', index = False)
#closed_fixed.to_csv('Clean_Data.csv', index = False)
#print('Generating Clean_Data File Ends')
closed_fixed.loc[closed_fixed['BREACH'] == 'YES', ['number','resolved_by','priority', 'BREACH']].to_excel(sla_breached_file_name, index = 0)
closed_fixed.loc[closed_fixed['BREACH'] == 'NO', ['number','resolved_by','priority', 'BREACH']].to_excel(sla_met_file_name, index = 0)
closed_fixed.loc[closed_fixed['BREACH'] == 'YES', ['number','resolved_by','priority', 'BREACH','resolution_time', 'resolved_by', 'Assignee_Email']].sort_values(by=['resolution_time'], ascending = False).head(5).to_excel(top_five_sla_miss, index = 0)
closed_fixed.loc[closed_fixed['BREACH'] == 'NO', ['number','resolved_by','priority', 'BREACH', 'resolution_time','resolved_by', 'Assignee_Email']].sort_values(by=['resolution_time'], ascending = False).head(5).to_excel(top_five_sla_met, index = 0)