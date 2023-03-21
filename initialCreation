from tableauhyperapi import Connection, HyperProcess, SqlType, TableDefinition, \
    escape_string_literal, escape_name, NOT_NULLABLE, Telemetry, Inserter, CreateMode, TableName
import pyodbc, requests, json
from requests.packages.urllib3.fields import RequestField
from requests.packages.urllib3.filepost import encode_multipart_formdata

# Open database connection and prepare a cursor.  This demo script is built for SQL Server, so modify as needed for your database of choice.
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      'SERVER=<database-url>;'
                      'DATABASE=<database-name>;'
                      'UID=<username>;'
                      'PWD=<password>;'
                      'trust_server_certificate=yes;')
cursor = cnxn.cursor()


# execute SQL queries using execute() and fetchall()
cursor.execute("SELECT [Row ID], [Order ID], [Profit], [Sales], [Region] FROM [Superstore].[dbo].[Orders]")
sales_data=cursor.fetchall()
cursor2=cnxn.cursor()
cursor2.execute("SELECT [Person],[Region] FROM [Superstore].[dbo].[People]")
entitlements_data = cursor2.fetchall()

#turn retrieved data into a 2-table Hyper schema.  I'm pushing this to my Desktop, so customize your file path as needed
with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, r'C:\Users\fgunning\Desktop\salesRLS.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        print("The connection to the Hyper file is open.")
        connection.catalog.create_schema('sales')
        sales_table = TableDefinition(TableName('sales','fact'), [
            TableDefinition.Column('Row ID', SqlType.double()),
            TableDefinition.Column('Order ID', SqlType.text()),
            TableDefinition.Column('Sales', SqlType.double()),
            TableDefinition.Column('Profit', SqlType.double()),
            TableDefinition.Column('Region', SqlType.text())
         ])
        print("Sales table is defined.")
        entitlements_table = TableDefinition(TableName('sales','entitlement'), [
            TableDefinition.Column('Person', SqlType.text()),
            TableDefinition.Column('Region', SqlType.text())
         ])
        print("Entitlements table is defined")
        connection.catalog.create_table(sales_table)
        connection.catalog.create_table(entitlements_table)
        print("Both tables created")
        with Inserter(connection, sales_table) as inserter:
            inserter.add_rows(sales_data)
            inserter.execute()
        with Inserter(connection, entitlements_table) as inserter:
            inserter.add_rows(entitlements_data)
            inserter.execute()
        
        print("The data was added to the tables.")
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")
