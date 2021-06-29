#coucou
import json
import requests
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)



def connect_to_google_discovery_service(service, version):

    import googleapiclient.discovery

    http = authorize_http_instance()
    service = googleapiclient.discovery.build(service, version, discoveryServiceUrl='https://displayvideo.googleapis.com/$discovery/rest?version=v1', http=http)
    return service


def get_sdf(advertiser_id, io_ids, file_type):

    import googleapiclient.http
    import io 
    import csv
    from zipfile import ZipFile
    import time

    def create_download_sdf_task():


        createSdfDownloadTaskRequest = {'version': 'SDF_VERSION_UNSPECIFIED', 'advertiserId': advertiser_id, 'parentEntityFilter': {'fileType':  file_type, 'filterType': 'FILTER_TYPE_INSERTION_ORDER_ID', 'filterIds':  io_ids}}
        operation = service.sdfdownloadtasks().create(body=createSdfDownloadTaskRequest).execute();

        get_request = service.sdfdownloadtasks().operations().get(name=operation["name"])

        start_time = time.time()
        while True:
          operation = get_request.execute()
          if(time.time() - start_time > 350): raise ValueError('Taking too long to fetch SDF')
          if('done' in operation):
            return operation['response']['resourceName']


    def download_sdf():

        downloadRequest = service.media().download_media(resourceName=resource_name)

        stream = io.BytesIO()
        downloader = googleapiclient.http.MediaIoBaseDownload(stream, downloadRequest)
        download_finished = False
        while download_finished is False:
          _, download_finished = downloader.next_chunk()

        data = []

        with ZipFile(stream).open(ZipFile(stream).namelist()[0],'r') as csvfile:
            decoded_file = csvfile.read().decode('utf-8').splitlines()
            csvreader=csv.reader(decoded_file)

            for num,line in enumerate(csvreader):
                if num==0:
                    keys = line #headers row
                else:
                    if line[0] != '': data.append(dict(zip(keys, line)))

        df = pd.DataFrame(data)
        return df

    print('Fetching SDFs...')

    service = connect_to_google_discovery_service('displayvideo', 'v1')
    resource_name = create_download_sdf_task()
    # resource_name = 'sdfdownloadtasks/media/14751297'
    df = download_sdf()
    return df



def dcm_report_to_df(report):

    import io

    report_list = report.splitlines()
    cut_at = [i for (i, x) in enumerate(report_list) if x=='Report Fields'][0]+1
    report = report_list[cut_at:]
    report = '\n'.join(report)
    report = io.StringIO(report)
    df = pd.read_csv(report)
    df = df[0:-1]
    # df['Date'] = pd.to_datetime(df['Date'])

    return df



def authorize_http_instance():

    from oauth2client.client import AccessTokenCredentials
    import httplib2

    user_agent =  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'

    credentials = AccessTokenCredentials(get_google_access_token(), user_agent)
    http = credentials.authorize(httplib2.Http())
    return http


def connect_to_pandas_gbq(project_name):

    import pandas_gbq
    from google.oauth2 import service_account
    
    gcp_credentials = service_account.Credentials.from_service_account_file('credentials/service-account.json', scopes=["https://www.googleapis.com/auth/cloud-platform"])

    pandas_gbq.context.credentials = gcp_credentials
    pandas_gbq.context.project = project_name


def sdf_to_df(data):
    data = data.split('\n')
    data = [x.split(',') for x in data]
    column_names = data.pop(0)
    df = pd.DataFrame(data, columns = column_names)
    df = df[~df['Name'].isnull()]
    return df


def connect_to_gsheets():
    import gspread
    from google.oauth2 import service_account
    gs = gspread.service_account('credentials/service-account.json')
    return gs


def divide_list_in_chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))


def connect_to_google_big_query():

    from google.oauth2 import service_account
    from google.cloud import bigquery

    gcp_credentials = service_account.Credentials.from_service_account_file('credentials/service-account.json', scopes=["https://www.googleapis.com/auth/cloud-platform"])
    bq_client = bigquery.Client(credentials=gcp_credentials,project=gcp_credentials.project_id)

    return bq_client


def connect_to_google_storage():

    from google.oauth2 import service_account

    gcp_credentials = service_account.Credentials.from_service_account_file('credentials/service-account.json', scopes=["https://www.googleapis.com/auth/cloud-platform"])
    storage_client = storage.Client(credentials=gcp_credentials,project=gcp_credentials.project_id)

    return storage_client



def get_google_access_token():


    f = open('credentials/service-account.json')
    client_secrets = json.load(f)
    client_id, client_secret, refresh_token = client_secrets['web']['client_id'], client_secrets['web']['client_secret'], client_secrets['web']['refresh_token']

    params = { "grant_type": "refresh_token", "client_id": client_id, "client_secret": client_secret,"refresh_token": refresh_token}
    authorization_url = "https://www.googleapis.com/oauth2/v4/token"
    r = requests.post(authorization_url, data=params)
    return r.json()['access_token']


def get_headers():
    headers = {'authorization': 'Bearer ' + get_google_access_token(), 'Content-Type': 'application/json'}
    return headers

def get_google_api_endpoint(token, url, method='GET'):

	headers = {'authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
	response = requests.request(method, url, headers=headers)
	response = json.loads(response.content) 
	return response

def dv360_patch(token, url, data):
    headers = {'authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
    response = requests.patch(url, headers=headers, json=data)
    response = json.loads(response.content) 
    return response

