import requests
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool
from urllib.parse import quote
import json

class PsetApi:

    def __init__(self, authentication, project_id=None):
        self.authentication = authentication
        self.headers = {
            "Authorization": f"Bearer {authentication.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.BASE_URL = self.authentication.endpoints['pset']
        self.project_id = project_id

    def get_lib_defs(self, lib_id):
        response = requests.get(
            f"{self.BASE_URL}libs/{lib_id}/defs",
            headers=self.headers,
        )
        return response.json()

    def encoder(self,decoded_str):
        encoded_str = quote(decoded_str, safe="")
        return encoded_str

    def frn_notation(self, object_id, modelId = None, versionId = None):
        '''
        object_id is the ifc GUID
        modelId is the tc id of the model
        versionId is the tc id of the version
        '''
        if modelId and versionId and object_id:
            notation = self.encoder(f"frn:tcfile:{modelId}:{versionId}/entity:{object_id}")
        elif modelId and object_id:
            notation = self.encoder(f"frn:tcfile:{modelId}/entity:{object_id}")
        elif versionId and object_id:
            notation = self.encoder(f"frn:tcfile:{versionId}/entity:{object_id}")
        elif object_id:
            notation = self.encoder(f"frn:entity:{object_id}")
        elif modelId:
            notation = self.encoder(f"frn:tcfile:{modelId}")
        elif versionId:
            notation = self.encoder(f"frn:tcfile:{versionId}")
        elif modelId and versionId:
            notation = self.encoder(f"frn:tcfile:{modelId}:{versionId}")
        return notation

    def get_object_psets(self, object_id, modelId = None, versionId = None):
        '''
        object_id is the ifc GUID
        modelId is the tc id of the model
        versionId is the tc id of the version
        '''
        notation = self.encoder(self.frn_notation(object_id, modelId, versionId))
        response = requests.get(
            f"{self.BASE_URL}psets/{notation}",
            headers=self.headers,
        )
        return response.json()
        
    def update_pset(self, props, libId, defId, object_id, modelId=None, versionId=None, headers=None, new=False):
        # Refresh tokens if needed before making the API call
        
        if headers is None:
            headers = {
                "Authorization": f"Bearer {self.authentication.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        else:
            headers = headers
            
        notation = self.frn_notation(object_id, modelId,versionId)
        print(notation)
        if new == True:
            headers['If-None-Match'] = '*'
            
        response = requests.patch(
            f"{self.BASE_URL}psets/{notation}/{libId}/{defId}",
            headers=headers,
            data=json.dumps(props),
        )
        return response.json()


    def prop_set_table(self,lib_defs):
        prop_nodes = lib_defs["items"][0]["schema"]["props"]
        props_names_and_keys = lib_defs["items"][0]["i18n"]["en-US"]["props"]
        df = pd.DataFrame(prop_nodes).T.reset_index().rename(columns={"index": "prop"})
        df['name'] = df['prop'].apply(lambda x: props_names_and_keys[x])
        df['libId'] = lib_defs['items'][0]['libId']
        df['defId'] = lib_defs["items"][0]["id"]
        df = df[['libId','defId','prop','name','type','required','default']]
        return df
    
    def create_library(self, data):
        response = requests.post(
            f"{self.BASE_URL}libs",
            headers=self.headers,
            data=json.dumps(data),
        )
        return response.json()
        
    
    def create_pset(self, data, libId):
        response = requests.post(
            f"{self.BASE_URL}libs/{libId}/defs",
            headers=self.headers,
            data=json.dumps(data),
        )
        return response.json()


    def update_pset_wrapper(self, args):
        # Unpacking the arguments tuple and calling the `update_pset` method
        return self.update_pset(*args)

    def mp_helper(self, objects):
        with Pool(8) as p:
            # Using `update_pset_wrapper` to ensure the arguments are unpacked correctly
            results = list(tqdm(p.imap(self.update_pset_wrapper, objects), total=len(objects)))
        return results