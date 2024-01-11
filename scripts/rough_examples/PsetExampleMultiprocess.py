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
file_api = TrimbleFileApi(authentication=auth,base_url=file_api_endpoint)
projects = file_api.get_projects()
project_id = projects[0]["id"]
org_api = OrgApi(authentication=auth, project_id=project_id, base_url=org_api_endpoint)
pset_api = PsetApi(authentication=auth, project_id=project_id, base_url=pset_api_endpoint)

static_token = auth.get_static_token()
# multiprocessing version


def to_float(x):
    try:
        return float(x)
    except:
        return None


def to_bool(x):
    try:
        return bool(x)
    except:
        return None


def null_to_negative_9999999(x):
    if x is None:
        return -99999.999
    else:
        return x


def update_pset_mp(args):
    index, row = args[0]  # Extract index and row from the passed args tuple
    if row['GUID'] in open('results.txt').read():
        return 'skipped'
    pset_df = args[1]
    sub_row = row[[col for col in row.index if col in pset_df.ref_name.tolist()]]
    # the order may be different, so we need to match the order of the df
    sub_row = sub_row.reindex(pset_df.ref_name.tolist())
    # to dict
    props = zip(pset_df.prop.tolist(), pset_df.type.tolist(), sub_row.values)
    props = pd.DataFrame(props, columns=["prop", "type", "value"])
    props = props[props.value.notnull()]
    # if the props type for that row is number, we need to convert to float and create None if it doesn't exist
    props.loc[props.type == "number", "value"] = props[props.type == "number"][
        "value"
    ].apply(lambda x: to_float(x) if x != "" else None)
    # if boolean, convert to boolean
    props.loc[props.type == "boolean", "value"] = props[props.type == "boolean"][
        "value"
    ].apply(lambda x: to_bool(x) if x != "" else None)
    props = props[["prop", "value"]].set_index("prop").to_dict()
    props = props["value"]
    pset_update_object = {
        "props": props,
    }
    try:
        results = pset_api.update_pset(
            props=pset_update_object,
            libId=pset_df.libId.tolist()[0],
            defId=pset_df.defId.tolist()[0],
            object_id=row["GUID"],
            modelId=row["model_id"],
            # versionId=row['versionId'],
            # new=True,
        )
        print(results)
        with open('results.txt', 'a+') as f:
            f.write(f"{row["GUID"]}\n")
        return results
    except Exception as e:
        # auth.renew_tokens()
        # update_pset_mp(args)
        with open('failed.txt', 'a+') as f:
            f.write(f'{row["GUID"]} : failed\n')
        return e



def get_pset_df():
    forestId = f"project:{project_id}:data"  # frn notation for project data forest
    discovery_trees = org_api.get_discovery_trees(forestId)
    treeId = discovery_trees["items"][0]["id"]
    nodes = org_api.get_nodes(forestId, treeId)
    nodeId = nodes["items"][0]["id"]
    node = org_api.get_node(forestId, treeId, nodeId)
    libIds = node["links"]
    libIds = [libId.split(":")[2] for libId in libIds]
    lib_defs = pset_api.get_lib_defs(libIds[1])
    df = pset_api.prop_set_table(lib_defs)
    df["ref_name"] = ["SOL_Volume", "NEL_Volume", "TRIM_Volume", "vol_flag"]
    return df


def convert_to_float(x):
    try:
        return float(x)
    except:
        return None

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

def get_vol_df():
    table_name = "Geometry_Consolidated"
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
    vol = vol[vol.model_name =='NEL-0001.ifc']
    vol[["NEL_Volume", "TRIM_Volume", "SOL_Volume"]] = vol[
        ["NEL_Volume", "TRIM_Volume", "SOL_Volume"]
    ].applymap(convert_to_float)
    vol[["NEL_Volume", "TRIM_Volume", "SOL_Volume"]] = vol[
        ["NEL_Volume", "TRIM_Volume", "SOL_Volume"]
    ].applymap(null_to_negative_9999999)
    vol['vol_flag'] = vol.apply(calculate_vol_flag, axis=1)
    for index, row in vol.iterrows():
        flag = row["vol_flag"]
        if flag == "r":
            vol.loc[index, "vol_flag"] = True
        elif flag == "g":
            vol.loc[index, "vol_flag"] = False
        else:
            vol.loc[index, "vol_flag"] = None
    vol.reset_index(drop=True, inplace=True)
    return vol

# we want to pass through a list of dicts to the api

if __name__ == "__main__":
    if not os.path.exists('results.txt'):
        with open('results.txt','w') as fp:
            pass
    print('getting pset df')
    pset_df = get_pset_df()
    print('getting vol df')
    vol = get_vol_df()
    print('setting up args')
    # vol from 6394 onwards
    print(vol["vol_flag"].value_counts())
    models = vol["versionId"].unique().tolist()
    for model in models:
        while True:
            processed = open('results.txt').read().splitlines()
            vol_model = vol[vol["versionId"] == model]
            vol_model = vol_model[~vol_model["GUID"].isin(processed)]
            vol_model.reset_index(drop=True, inplace=True)
            if vol_model.shape[0] == 0:
                break
            else:
                try:
                    auth.renew_tokens()
                    vol_model = vol[vol["versionId"] == model]
                    vol_model = vol_model[~vol_model["GUID"].isin(processed)]
                    vol_model.reset_index(drop=True, inplace=True)
                    args = [((i, row), pset_df) for i, row in vol_model.iterrows()]
                    with Pool(8) as p:
                        results = list(tqdm(p.imap(update_pset_mp, args), total=vol_model.shape[0]))
                    with open('NEL-0001.txt','a+') as f:
                        print(results, file=f)
                except Exception as e:
                    print(e)
                    pass
