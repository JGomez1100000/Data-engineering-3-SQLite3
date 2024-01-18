import sqlite3
import pandas as pd

conn = sqlite3.connect('STAFF.db')

table_name_in = 'INSTRUCTOR'
attribute_list1 = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

table_name_dp = 'DEPARTMENTS'
attribute_list2 = ['DEPT_ID', 'DEPT_NAME', 'MANAGER_ID', 'LOC_ID']

file_path1 = r'C:\Users\Javi\Documents\GitHub\SQLite3\INSTRUCTOR.csv'
df_in = pd.read_csv(file_path1, names = attribute_list1)

file_path2 = r'C:\Users\Javi\Documents\GitHub\SQLite3\Departments.csv'
df_dp = pd.read_csv(file_path2, names = attribute_list2)

df_in.to_sql(table_name_in, conn, if_exists = 'replace', index =False)
print('Table1 is ready \n')

df_dp.to_sql(table_name_dp, conn, if_exists = 'replace', index =False)
print('Table2 is ready \n')

# Query
query_statement = f"SELECT * FROM {table_name_in}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT FNAME FROM {table_name_in}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT COUNT(*) FROM {table_name_in}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


# New data
data_dict1 = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append1 = pd.DataFrame(data_dict1)

data_append1.to_sql(table_name_in, conn, if_exists = 'append', index =False)
print('Data appended successfully \n')

query_statement = f"SELECT * FROM {table_name_in}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)




query_statement = f"SELECT * FROM {table_name_dp}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT DEPT_NAME FROM {table_name_dp}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT COUNT(*) FROM {table_name_dp}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

data_dict2 = {'DEPT_ID' : [9],
            'DEPT_NAME' : ['Quality Assurance'],
            'MANAGER_ID' : ['30010'],
            'LOC_ID' : ['L0010']}

data_append2 = pd.DataFrame(data_dict2)

data_append2.to_sql(table_name_dp, conn, if_exists = 'append', index =False)
print('Data appended successfully \n')

query_statement = f"SELECT * FROM {table_name_dp}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()