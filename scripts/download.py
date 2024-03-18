from TrimblePy.common.auth import Authentication
from TrimblePy.connect.file_api import TrimbleFileApi
import pandas as pd
from io import StringIO

region = 'AP'

auth = Authentication(sql_available=True, token_retrieval_method='env',region=region) 
access_token, refresh_token = auth.get_token()

file_api = TrimbleFileApi(authentication=auth)
projects = file_api.get_projects()
project_id = projects[0]['id']

# -----------------------------------------------------------------
# PROJECTS
# -----------------------------------------------------------------


file_api = TrimbleFileApi(authentication=auth,project_id=project_id)

project_file_prefix = "NEL-STH-NSA"

try:
    files = file_api.get_files()
except Exception as e:
    print(f"An error occurred: {e}")

# /files/fs/{fileId}/downloadurl
files_list_of_dicts = [vars(file_obj) for file_obj in files.values()]
file_df = pd.DataFrame(files_list_of_dicts)

# folder
folder_df = file_df[file_df.full_path=='/99 Working/JP/CH']
dl_urls = []
for id in folder_df.id:
    dl_urls.append(file_api.download_url(id))
    
folder_df['dl_url'] = dl_urls

# csv
id = file_df[file_df['name'].str.contains('csv')].iloc[20].id
file = file_api.download(id)
text = file.decode('utf-8')
df = pd.read_csv(StringIO(text), lineterminator='\n', header=1)


# excel
id = file_df[file_df['name'].str.contains('xlsx')].iloc[20].id
file = file_api.download(id)
df = pd.read_excel(file)

# xml
import lxml.etree as ET
id = file_df[file_df['name'].str.contains('xml')].iloc[20].id
file = file_api.download(id)
# x00v\x00a\x00l\x00u\x00e\x00>\x00\r\x00\n\x00 \x00 \x00 \x00 \x00 \x00 \x00 \x00 \x00 \x00
decoded_file = file.decode('utf-16')
tree = ET.parse(StringIO(decoded_file))
number_of_lines = len(decoded_file.split('\n')) 

# ifc
import ifcopenshell
id = file_df[file_df['name'].str.contains('ifc')].iloc[20].id
file = file_api.download(id)
# in bytes = convert to string
string_file = StringIO(file.decode('utf-8')).read()
# all \r and \n should be converted to 
model = ifcopenshell.open
# bcf

