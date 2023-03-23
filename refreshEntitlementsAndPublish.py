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
cursor2=cnxn.cursor()
cursor2.execute("SELECT [Person],[Region] FROM [Superstore].[dbo].[People]")
entitlements_data = cursor2.fetchall()

#turn retrieved data into a 2-table Hyper schema.  I'm pushing this to my Desktop, so customize your file path as needed
with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print("The HyperProcess has started.")

    with Connection(hyper.endpoint, r'C:\Users\fgunning\Desktop\salesRLS.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        print("The connection to the Hyper file is open.")
        connection.catalog.create_schema('sales')
        entitlements_table = TableDefinition(TableName('sales','entitlement'), [
            TableDefinition.Column('Person', SqlType.text()),
            TableDefinition.Column('Region', SqlType.text())
         ])
        print("Entitlements table is defined")
        connection.catalog.create_table(entitlements_table)
        with Inserter(connection, entitlements_table) as inserter:
            inserter.add_rows(entitlements_data)
            inserter.execute()
        
        print("The data was added to the entitlements table.")
    print("The connection to the Hyper extract file is closed.")
print("The HyperProcess has shut down.")

#Establish a session with Tableau Server.
ts_username = '<tableau-username>'
ts_password = '<tableau-password'
ts_url = '<server-url>'
site = '<site-name>'

headers = {'accept': 'application/json','content-type': 'application/json'}
payload = { "credentials": {"name": ts_username, "password": ts_password, "site" :{"contentUrl": site} } }
req = requests.post(ts_url + 'api/3.5/auth/signin', json=payload, headers=headers, verify=True)
response =json.loads(req.content)
token = response["credentials"]["token"]
site_id = response["credentials"]["site"]["id"]
auth_headers = {'accept': 'application/json','content-type': 'application/json','x-tableau-auth': token}

#begin the file upload process
r = requests.post(ts_url + 'api/3.5/sites/'+site_id+'/fileUploads', headers=auth_headers)
upload_session_id = json.loads(r.content)['fileUpload']['uploadSessionId']

#Create a multipart upload.  For this sample, its only 1 chunk.  For better chunking info, see the sample
#scripts available at https://github.com/tableau/rest-api-samples/blob/master/python/publish_workbook.py
def _make_multipart(parts):
    mime_multipart_parts = []
    for name, (filename, blob, content_type) in parts.items():
        multipart_part = RequestField(name=name, data=blob, filename=filename)
        multipart_part.make_multipart(content_type=content_type)
        mime_multipart_parts.append(multipart_part)

    post_body, content_type = encode_multipart_formdata(mime_multipart_parts)
    content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
    return post_body, content_type
with open(r'C:\Users\fgunning\Desktop\salesRLS.hyper', 'rb') as f:
    data = f.read()
    payload, content_type = _make_multipart({'request_payload': ('', '', 'text/xml'),'tableau_file': ('file', data, 'application/octet-stream')})
    server_response = requests.put(ts_url + 'api/3.5/sites/'+site_id+'/fileUploads/'+upload_session_id, data=payload, headers={'x-tableau-auth': token, "content-type": content_type})
    
    #find target datasource.  I've hard-coded the name here as "livetohyper" so just replace it with your desired target datasource
get_datasources = requests.get(ts_url + 'api/3.5/sites/'+site_id+'/datasources?filter=name:eq:<datasource-name>',headers = auth_headers)
datasources_response =json.loads(get_datasources.content)
ds_id = datasources_response['datasources']['datasource'][0]['id']


#put the data into your datasource.  simply enumerate which tables in which schemas you want to replace

auth_headers = {'accept': 'application/json','content-type': 'application/json','x-tableau-auth': token, 'RequestID':'98'}

payload = {"actions": [{"action" : "replace",
                        "target-schema": "sales",
                       "target-table": "entitlement",
                        "source-schema":"sales",
                      "source-table": "entitlement"}]}
patch_request = requests.patch(ts_url + 'api/3.19/sites/'+site_id+'/datasources/'+ds_id+'/data?uploadSessionId='+upload_session_id, headers = auth_headers, json = payload)
print(patch_request)
