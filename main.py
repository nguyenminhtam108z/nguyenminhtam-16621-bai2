import csv
from mysql import connector
def mysqlconnect():
  conn = connector.connect(
    host='localhost',
    user='root',
    password="",
    db='subject',
  )
  return conn

def create_table():
  conn = mysqlconnect()
  with open('customer.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    fields = next(csvreader)
  query_create_table = "CREATE TABLE customers ("+fields[0]+" varchar(11) NOT NULL, "+fields[1]+" varchar(250) NOT NULL, "+fields[2]+" varchar(250), "+fields[3]+" varchar(250), "+fields[4]+" varchar(250),"+fields[5]+" varchar(250),"+fields[6]+"" \
                                                " varchar(250),"+fields[7]+" varchar(250), "+fields[8]+" varchar(50),"+fields[9]+" varchar(100),"+fields[10]+" varchar(20),"+fields[11]+" varchar(50) NOT NULL,"+fields[12]+" varchar(50) NOT NULL,PRIMARY KEY("+fields[0]+"))"
  cursor = conn.cursor()
  cursor.execute(query_create_table)
  print('create table customers successfully')
  cursor.close()
  conn.commit()
  conn.close()

def store_data():
  conn = mysqlconnect()
  cursor = conn.cursor()
  with open('customer.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    fields = next(csvreader)
    for i, row in enumerate(csvreader):
      if i != 0:
        query_insert_into_table_customers = "insert into customers("+fields[0]+","+fields[1]+","+fields[2]+","+fields[3]+","+fields[4]+","+fields[5]+","+fields[6]+","+fields[7]+","+fields[8]+","+fields[9]+","+fields[10]+","+fields[11]+","+fields[12]+")" \
                                            " values('" + (row[0]) + "','" + row[1] + "', '" + row[2] + "', '" + row[3] + "', '" + row[4] + "', '" + row[5] + "', '" + row[6] + "', '" + row[7] + "', '" + row[8] + "', '" + row[9] + "', '" + row[10] + "', '" + row[11] + "' ,'" + row[12] + "')"
        cursor.execute(query_insert_into_table_customers)
        conn.commit()
        print('insert complete')

  cursor.close()
  conn.close()
def run_queries(query):
  conn = mysqlconnect()
  cursor = conn.cursor()
  cursor.execute(query)
  print('table is deleted')
  cursor.close()
  conn.commit()
  conn.close()
def get_table(query):
  conn = mysqlconnect()
  cursor = conn.cursor()
  cursor.execute(query)
  data = cursor.fetchall()
  cursor.close()
  conn.commit()
  conn.close()
  return data
if __name__=='__main__':
    del_tabel = 'DROP TABLE customers'
    run_queries(del_tabel)
    create_table()
    store_data()
    data = get_table('select * from subject.customers')
    print('data=',data)

