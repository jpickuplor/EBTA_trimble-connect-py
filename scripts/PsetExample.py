'''
Volume data from Solibri, Trimble and modellers are compared and added to custom psets in Trimble Connect
'''


from TrimblePy.common.auth import Authentication
from TrimblePy.connect.file_api import TrimbleFileApi
from TrimblePy.org.org_api import OrgApi
from TrimblePy.pset.pset_api import PsetApi
from multiprocessing import Pool
import pandas as pd
from tqdm import tqdm
import os

auth = Authentication(sql_available=False, token_retrieval_method="env")
access_token, refresh_token = auth.get_token()

region = 'ap'  # the region you want to use
auth.set_base_url(region)

file_api_endpoint = auth.endpoints['tc']  # the base URL for the Trimble Connect API
model_api_endpoint = auth.endpoints['model']  # the base URL for the Trimble Connect Model API
topic_api_endpoint = auth.endpoints['topic']  # the base URL for the Trimble Connect Topic API
pset_api_endpoint = auth.endpoints['pset']  # the base URL for the Trimble Connect Pset API
org_api_endpoint = auth.endpoints['org']  # the base URL for the Trimble Connect Org API


# if it doesnt work - go through web to refresh everything
# auth.get_new_tokens_with_authorization_code()
file_api = TrimbleFileApi(authentication=auth)
projects = file_api.get_projects()
project_id = projects[0]["id"]
org_api = OrgApi(authentication=auth, project_id=project_id)
pset_api = PsetApi(authentication=auth, project_id=project_id)

forestId = f"project:{project_id}:data" # frn notation for project data forest

discovery_trees = org_api.get_discovery_trees(forestId)

treeId = discovery_trees["items"][0]["id"]

# OPTIONAL - get the tree
# tree = org_api.get_discovery_tree(forestId, treeId)

nodes = org_api.get_nodes(forestId, treeId)

nodeId = nodes["items"][0]["id"]

node = org_api.get_node(forestId, treeId, nodeId)

libIds = node["links"]
libIds = [libId.split(":")[2] for libId in libIds]


lib_defs = pset_api.get_lib_defs(libIds[1])

# Below is an example that updates psets of a model - using geometry from Solibri, Trimble Connect and custom project psets
# The lib_defs are the pset definitions from the library
# A simple table is created to map the pset definitions to the pset properties
# The geometry is retrieved from the SQL database - a common data environment for the project
# The data was not prepared for the exercise, so data preparation is done in the code

df = pset_api.prop_set_table(lib_defs)
df['ref_name'] = ['SOL_Volume','NEL_Volume','TRIM_Volume','vol_flag']

geo = pd.read_csv("geometry_20231213.csv")
vol = geo[
    [
        "GUID",
        "idx",
        "ifc_type",
        "name",
        "versionId",
        "model_name",
        "model_id",
        "NEL_Volume",
        "TRIM_Volume",
        "SOL_Volume",
        "vol_flag",
    ]
]

# model_list = [
#     "7CB3Fidn0m8"
# ]

# vol = vol[vol.model_id.isin(model_list)]
vol = vol[vol.GUID == "3umX$ru4UYGBPIgP5Wf3vx"]

# if NEL_Volume - SOL_Volume / NEL_Volume > 0.01, then vol_flag = r (absolute value of the difference is greater than 1%)

def convert_to_float(x):
    try:
        return float(x)
    except:
        return None

vol[['NEL_Volume','TRIM_Volume','SOL_Volume']] = vol[['NEL_Volume','TRIM_Volume','SOL_Volume']].apply(convert_to_float)


def calculate_vol_flag(row):
    try:
        nel_volume = row['NEL_Volume']
        trim_volume = row['TRIM_Volume']
        
        # Check for None or zero NEL_Volume to avoid division by zero
        if nel_volume is None or nel_volume == 0:
            return 'r'
        
        # Check if the relative difference is greater than 0.01
        if (nel_volume - trim_volume) / nel_volume > 0.01:
            return 'r'
        else:
            return 'g'
        
    except Exception as e:
        try:
            calculate_vol_flag(row)
        except:
            pass
        print(f"Error calculating vol_flag for row: {row}\nError: {e}")
        return None  # Return None or an appropriate value in case of error


vol['vol_flag'] = vol.apply(calculate_vol_flag, axis=1)

vol.loc[:,"vol_flag"] = vol["vol_flag"].apply(lambda x: True if x == "r" else False)
# vol = vol[vol.model_id=='QeKRcnHaUV0']
vol.reset_index(drop=True,inplace=True)

row = vol.loc[0]
# we want to pass through a list of dicts to the api

updates = []

for _, row in vol.iterrows():
    # match the props to ref_name - like where df['ref_name'] == row.index .. we should end up with df['prop'] and row.values
    sub_row = row[[col for col in row.index if col in df.ref_name.tolist()]]
    # the order may be different, so we need to match the order of the df
    sub_row = sub_row.reindex(df.ref_name.tolist())
    # to dict
    props = (zip(df.prop.tolist(), df.type.tolist(), sub_row.values))
    props = pd.DataFrame(props, columns=["prop", "type", "value"])
    props = props[props.value.notnull()]
    # if the props type for that row is number, we need to convert to float and create None if it doesn't exist
    props.loc[props.type == "number", "value"] = props[props.type == "number"]["value"].apply(
        lambda x: float(x) if x != "" else None
    )
    # if boolean, convert to boolean
    props.loc[props.type == "boolean", "value"] = props[props.type == "boolean"]["value"].apply(
        lambda x: bool(x) if x != "" else None
    )
    props = props[['prop','value']].set_index('prop').to_dict() 
    props = props['value']
    pset_update_object = {
        "props": props,
    }
    try:
        results = pset_api.update_pset(props=pset_update_object, libId=df.libId.tolist()[0], defId=df.defId.tolist()[0], object_id=row["GUID"])
        # print(row['GUID'],results['link'])
        updates.append(results)
    except Exception as e:
        updates.append(e)