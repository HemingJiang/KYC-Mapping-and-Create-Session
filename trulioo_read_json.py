import os
import csv
import time
from tendo import singleton
import csv, datetime, json, pandas
import pgpasslib
import psycopg2
from psycopg2 import sql
import configparser

dir = '/dwdata/kingslanding'
csv_file = 'trulioo_raw.csv'
csv_file_read_json = 'trulioo_test_read_json.csv'
dir_s3 = 'FILE PATH'

redshift_schema = 'kingslanding_testing'
redshift_table = 'trulioo_read_json'
copy_query = """
                    TRUNCATE %(redshift_schema)s.%(redshift_table)s;
                    COPY %(redshift_schema)s.%(redshift_table)s from '%(dir_s3)s/%(csv_file_read_json)s'
                        access_key_id '%(i)s'
                        secret_access_key '%(s)s'
                        region as 'ca-central-1'
                        IGNOREHEADER 1
                        REMOVEQUOTES
                        NULL AS 'NULL::timestamp'
                        DELIMITER '`'
                        COMPUPDATE ON; """



##########################################################
################ connext to redshift
passw = pgpasslib.getpass('AWS HOST ADDRESS',
  5439,
  'DATABASE',
  'USER')
conn_red = psycopg2.connect(
  host='AWS HOST ADDRESS',
  user='USER',
    dbname='DATABASE',
    password=passw,
    port=5439,
    connect_timeout=500)
cursor_red = conn_red.cursor()


##########################################################
################ get passw on S3 server
config = configparser.ConfigParser()
config.read('/dwdata/config.cfg')
access_key_id = config.get('s3','access_key_id')
secret_access_key = config.get('s3','secret_access_key')



##########################################################
################ pull %(redshift_schema)s.trulioo_logs data and save to csv and read
def importTableCSV(filepath):
    '''
    Uses the DictReader to come up with a dictionary output for easier manipulation on each row later.

    filepath is relative path to the .csv file
    '''
    output = []
    cursor_red.execute(""" SELECT * FROM kingslanding_testing.trulioo_logs; """)
    conn_red.commit
    df = pandas.DataFrame(cursor_red.fetchall())
    if len(df.index) > 0:
        df.columns = ([row[0] for row in cursor_red.description])
        os.system('rm %(dir)s' \
                        %{'dir':filepath})
        df.to_csv("%(dir)s"%{"dir":filepath}, encoding='utf-8', sep = '`', index = False, date_format='%Y-%m-%d %H:%M:%S')


    with open(filepath, 'r') as csvfile:
        reader = csv.DictReader(csvfile,delimiter = '`')
        for row in reader:
            output.append(row)

    return output


##########################################################
################ json string flatten and save to csv2 and return dataframe and push to s3 server
def flattenTableRows(csvarray):

    '''
    csvarray is a dictionary passed through importTableCSV
    '''

    # print('length of csvarray, ', len(csvarray))

    '''
    It's important to have this outside because we're going to get the complete superset of ALL fields from all datasources and all known countries for that particular CSV file.
    '''
    fieldnames = []

    '''
    We create a global output, and then write all at once at the end of the file.
    '''
    output = []

    for csvrow in csvarray:

        data = json.loads(csvrow.get('response'))

        record = data.get('Record')

        rows = record.get('DatasourceResults')
        # print('length of rows, ', len(rows))

        for row in rows:

            # print('\n\nNEW ROW')

            # Consolidate all into key:value format
            fields = row['DatasourceFields']
            payload = {
                field['FieldName']:field['Status'] for field in fields
                }

            # Add the datasource
            payload['datasource'] = row['DatasourceName']

            # Store transaction level data columns
            payload['country'] = data['CountryCode']
            payload['transaction_id'] = data['TransactionID']
            payload['upload_time'] = data['UploadedDt']

            # Record level data
            payload['transaction_record_id'] = record['TransactionRecordID']
            payload['record_status'] = record['RecordStatus']

            # csvrow level data
            payload['user_id'] = csvrow['user_id']
            payload['match_rules'] = record['Rule']['Note']
            payload['method'] = csvrow['method']
            payload['log_id'] = csvrow['id']

            # Add the country

            # print('payload is: ', payload)

            # Sometimes, for individual data sources, we have extra fields that can be addded (i.e. "deceased" = are they dead?)

            xfields = row['AppendedFields']
            extra_payload = {
                xfield['FieldName'] : xfield['FieldName'] for xfield in xfields
            }

            # print('extra is: ', payload)

            payload.update(extra_payload)

            # print('final payload is: ', payload)

            if payload:
                for key in payload.keys():
                    if key not in fieldnames:
                        fieldnames.append(key)

                # print('\nAPPEND PAYLOAD --> ')

                output.append(payload)

    return output
    # '''
    # Write everyting to a CSV.
    # '''
    #
    #
    # df_to_csv = pandas.DataFrame(output)
    # df_to_csv.to_csv("%(dir)s/%(csv_file_read_json)s"%{"dir":dir,"csv_file_read_json":csv_file_read_json}, encoding='utf-8', sep = '`', index = False, date_format='%Y-%m-%d %H:%M:%S')
    # os.system('s3cmd del %(dir_s3)s/%(csv_file_read_json)s' \
    #                 %{'dir_s3':dir_s3,"csv_file_read_json":csv_file_read_json})
    #
    # os.system('s3cmd put "%(dir)s/%(csv_file_read_json)s" "%(dir_s3)s/"' \
    #                 %{'dir':dir , "csv_file_read_json":csv_file_read_json, 'dir_s3':dir_s3})
    # return df_to_csv


##########################################################
################ create rule match with category_3kyc_v3 and save to csv2 and return dataframe and push to s3 server

def category_3kyc_v3(matchtorule):
    '''
    # created lists of countries for different countries rules
    # countries_1 in : AT, BE, DK, FR, NO, ES, PT
    # countries_2 in : IT
    # countries_3 in : NL
    # countries_4 in : GB
    '''
    countries_1 = ['AT','BE','DK','FR','NO','ES','PT']
    countries_2 = ['IT']
    countries_3 = ['NL']
    countries_4 = ['GB']
    id_datasources = ['passport','license','number']


    '''
    Find out if the full name is match first
    '''
    for source in matchtorule:
        source['name_match'] =0
        source['dob_match'] =0
        source['address_match'] =0
        source['id_match'] =0
        print(source['country'],source['user_id'])
        if ('FirstGivenName' in list(source.keys()) and source['FirstGivenName'] == 'match' and              'FirstSurName' in list(source.keys()) and source['FirstSurName'] == 'match') or         (source['country'] in countries_3              and ( ('FirstInitial' in list(source.keys()) and source['FirstInitial'] == 'match') or                    ('GivenInitials' in list(source.keys()) and source['GivenInitials'] == 'match') or                    ('GivenNames' in list(source.keys()) and source['GivenNames'] == 'match' and                  source['GivenMames'] != 'nomatch') or                    ('FirsGivenName' in list(source.keys()) and source['FirstGivenName'] == 'match' and                  source['FirstGivenName'] != 'nomatch'))              and ('LastName' in list(source.keys()) and source['LastName'] == 'match')) :
            source['name_match'] = 1

    '''
    #check data sources
    '''
        #    if all(i not in source['datasource'].lower() for i in id_datasources):


            #print('here')

        if 'DayOfBirth' in list(source.keys()) and source['DayOfBirth'] == 'match':
            source['dob_match'] = 1

    '''
    # if the full name is match but dob is not match, go to countries
    '''
        if source['country'] in countries_1 and (('Ciry' in list(source.keys()) and source['Ciry'] == 'match') or ('PostalCode' in list(source.keys()) and source['PostalCode'] == 'match')) and ('HouseNumber' in list(source.keys()) and source['HouseNumber'] == 'match') and ('StreetName' in list(source.keys()) and source['StreetName'] == 'match') :
            source['address_match'] = 1


        if source['country'] in countries_2 and (('Ciry' in list(source.keys()) and source['Ciry'] == 'match') or ('PostalCode' in list(source.keys()) and source['PostalCode'] =='match')) and 'HouseNumber' in list(source.keys()) and source['HouseNumber'] == 'match' and 'StreetName' in list(source.keys()) and source['StreetName'] == 'match' and 'UnitNumber' in list(source.keys()) and source['UnitNumber'] != 'nomatch':
            source['address_match'] = 1

        if source['country'] in countries_3 and (('PostalCode' in list(source.keys()) and source['PostalCode'] == 'match' and 'HouseNumber' in list(source.keys()) and source['HouseNumber'] == 'match' ) or ('HouseNumber' in list(source.keys()) and source['HouseNumber'] == 'match' and 'StreetName' in list(source.keys()) and source['StreetName'] == 'match' and 'Ciry' in list(source.keys()) and source['Ciry'] == 'match')):
            source['address_match'] = 1

        if source['country'] in countries_4 and 'PostalCode' in list(source.keys()) and 'StreetName' in list(source.keys()) and source['PostalCode'] == 'match' and source['StreetName'] == 'match' and (('BuildingName' in list(source.keys()) and source['BuildingName'] == 'match') or ('BuildingNumber' in list(source.keys()) and source['BuildingNumber'] == 'match')):
            source['address_match'] = 1


    '''
    # if full name is match but dob and address are not match, go to id
    #id match: (DriversLicense
    #                         OR MedicareNumber
    #                         OR MxCurp
    #                         OR NationalIDNumber
    #                         OR NHSNumber
    #                         OR NINumber
    #                         OR PassportNumber
    #                         OR PersonalPublicServiceNumber
    #                         OR RegistrationNumber
    #                         OR SePinNumber
    #                         OR SgNRICNumber
    #                         OR SocialInsuranceNumber
    #                         OR SocialSecurityNumber)
    '''

        if ( 'DriversLicense' in list(source.keys()) and source['DriversLicense'] =='match') or ( 'MedicareNumber' in list(source.keys()) and source['MedicareNumber'] =='match') or ( 'MxCurp' in list(source.keys()) and source['MxCurp'] =='match') or( 'NationalIDNumber' in list(source.keys()) and source['NationalIDNumber'] =='match') or ( 'NHSNumber' in list(source.keys()) and source['NHSNumber'] =='match') or ( 'NINumber' in list(source.keys()) and source['NINumber'] =='match') or ( 'PassportNumber' in list(source.keys()) and source['PassportNumber'] =='match') or ( 'PersonalPublicServiceNumber' in list(source.keys()) and source['PersonalPublicServiceNumber'] =='match') or ( 'RegistrationNumber' in list(source.keys()) and source['RegistrationNumber'] =='match') or ( 'SePinNumber' in list(source.keys()) and source['SePinNumber'] =='match') or ( 'SgNRICNumber' in list(source.keys()) and source['SgNRICNumber'] =='match') or ( 'SocialInsuranceNumber' in list(source.keys()) and ource['SocialInsuranceNumber'] =='match') or ( 'SocialSecurityNumber' in list(source.keys()) and source['SocialSecurityNumber'] =='match'):
            source['id_match'] = 1


    df_to_csv = pandas.DataFrame(matchtorule)
    df_to_csv.to_csv("%(dir)s/%(csv_file_read_json)s"%{"dir":dir,"csv_file_read_json":csv_file_read_json}, encoding='utf-8', sep = '`', index = False, date_format='%Y-%m-%d %H:%M:%S')
    os.system('s3cmd del %(dir_s3)s/%(csv_file_read_json)s' \
                    %{'dir_s3':dir_s3,"csv_file_read_json":csv_file_read_json})

    os.system('s3cmd put "%(dir)s/%(csv_file_read_json)s" "%(dir_s3)s/"' \
                    %{'dir':dir , "csv_file_read_json":csv_file_read_json, 'dir_s3':dir_s3})
    return df_to_csv




df_output = category_3kyc_v3(flattenTableRows(importTableCSV("%(dir)s/%(csv_file)s"%{"dir":dir,"csv_file":csv_file})))
query_drop_create = """
    DROP TABLE IF EXISTS %(redshift_schema)s.%(redshift_table)s ;
    CREATE TABLE IF NOT EXISTS %(redshift_schema)s.%(redshift_table)s (
    %(query_create_column)s
    );

    GRANT SELECT ON  %(redshift_schema)s.%(redshift_table)s to etl_pipeline;
    GRANT SELECT ON  %(redshift_schema)s.%(redshift_table)s to klipfolio;
    """

query_create_column = []
column_list = list(df_output)
print(column_list)

for i in range(len(column_list)):
    if i < len(column_list) - 1:
        query_create_column.append("%(column)s VARCHAR(1000) DEFAULT NULL,"%{"column":str(column_list[i].lower())})
    else:
        query_create_column.append("%(column)s VARCHAR(1000) DEFAULT NULL"%{"column":str(column_list[i].lower())})
# print(''.join(query_create_column))
cursor_red.execute(query_drop_create%{"redshift_schema":redshift_schema,"redshift_table":redshift_table,"query_create_column":''.join(query_create_column)})
conn_red.commit()
cursor_red.execute(copy_query%{"redshift_schema":redshift_schema,"redshift_table":redshift_table,"dir_s3":dir_s3,"csv_file_read_json":csv_file_read_json,"i": access_key_id, "s": secret_access_key})
conn_red.commit()

conn_red.close()
