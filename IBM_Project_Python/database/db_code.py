import sqlite3
import pandas as pd

conn = sqlite3.connect('STAFF.db')
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

df = pd.read_csv(r'D:\study\Projects\Data_Engineering\IBM_Project_Python\database\INSTRUCTOR.csv', names=attribute_list)

df.to_sql(table_name, conn, if_exists='replace', index=False)
print('table is ready')

query_statement = f'SELECT * FROM {table_name}'
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists='append', index=False)

conn.close()
