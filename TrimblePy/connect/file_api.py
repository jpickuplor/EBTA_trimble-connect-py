import requests
import pandas as pd
import time

class TrimbleFileApi:

    def __init__(self, authentication, project_id=None):
            """
            Initializes a new instance of the FileAPI class.

            Parameters:
            authentication (Authentication): An instance of the Authentication class.

            """
            self.authentication = authentication
            self.BASE_URL = self.authentication.endpoints['tc']
            self.headers = {
                "Authorization": f"Bearer {self.authentication.access_token}",
                "Accept": "application/json"
            }
            self.project_id = project_id

    def get_projects(self, fullyLoaded=True, minimal=False, sort='-lastVisited'):
        '''
        fullyLoaded: Include file/folder details in the listing (No of files, No of Folders, No of File Versions, Size), commerce access info. Default is TRUE
        minimal: Retrieve only id, name of the project in the listing. Will override fullyLoaded parameter. Default is FALSE
        sort: Supported sorting fields are "name", "size", "lastVisited", "modified", "updatedOn". Prefix with "+" for ascending and "-" for descending sort order. Default is "-lastVisited"
        '''
        url = f'{self.BASE_URL}projects?fullyLoaded={fullyLoaded}&minimal={minimal}&sort={sort}'
        response = requests.get(url, headers=self.headers)
        return response.json()
        
        

    def get_file_snapshot(self):
        url = f'{self.BASE_URL}files/fs/snapshot?projectId={self.project_id}&includeDeleted=false&includeAttachment=false&maxItems=100000'
        response = requests.get(url, headers=self.headers)
        return response.json()
    
    def build_index(self, dfs):
        id_to_parent = dfs.set_index('id')['parentId'].to_dict()
        id_to_name = dfs.set_index('id')['name'].to_dict()
        return id_to_parent, id_to_name

    def get_full_path(self, row, id_to_parent, id_to_name):
        path = []
        parentId = row['parentId']
        while parentId:
            path.insert(0, id_to_name.get(parentId, ''))
            parentId = id_to_parent.get(parentId)
        return '/'.join(path)

    def download(self, id):
        file_download_url = f"{self.BASE_URL}files/fs/{id}/downloadurl"
        res = requests.get(file_download_url, headers=self.headers)
        file = requests.get(res.json()['url'],headers=self.headers)
        return file.content
    
    def download_url(self, id):
        file_download_url = f"{self.BASE_URL}files/fs/{id}/downloadurl"
        res = requests.get(file_download_url, headers=self.headers)
        return res.json()['url']

    def get_files(self):
        print("Getting File Snapshot From Trimble...")
        fs = self.get_file_snapshot()
        dfs = pd.json_normalize(fs['items'])
        dfs.rename(columns={
            "id": "id",
            "vid": "versionId",
            "nm": "name",
            "pid": "parentId",
            "ptp": "parentType",
            "tp": "fileType",
            "ct": "createdTime",
            "mt": "modifiedTime",
            "cid": "createdBy",
            "mid": "modifiedBy",
            "sz": "size",
            "del": "deleted",
            "md5": "md5",
            "rv": "revision",
            "chid": "checkoutBy",
            "cht": "checkoutTime",
            "tn": "thumbnail"
        }, inplace=True)

        files_df = dfs[dfs['fileType'] == 'FILE'].copy()
        print("Creating Full Path For Files...")
        id_to_parent, id_to_name = self.build_index(dfs)

        trimble_files = {}
        for _, file_data in files_df.iterrows():
            file_dict = file_data.to_dict()

            file_dict['full_path'] = self.get_full_path(file_dict, id_to_parent, id_to_name)
            file_dict['parent_folder'] = id_to_name.get(file_dict['parentId'])
            file_dict['deleted'] = file_dict.get('deleted', None)

            trimble_file = TrimbleFile(**file_dict)
            trimble_files[trimble_file.id] = trimble_file  # Store in dictionary with id as key

        print("Done!")
        return trimble_files

    def safe_request(self, url, max_retries=3):
        for i in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if i < max_retries - 1:
                    time.sleep(1)
                    continue
                else:
                    raise e

    def get_activities(self, inspected=None, max_depth=10000):
        if inspected is None:
            inspected_set = set()
        else:
            inspected_set = set(inspected) 
        url = f'{self.BASE_URL}activities?projectId={self.project_id}'
        response = self.safe_request(url)
        data_objects = []
        data = response.json()
        data_objects.extend(data)
        depth = 0
        while depth < max_depth:
            print(f"Getting page {depth} of activities...")
            lastId = data[-1]['id']
            if lastId in inspected_set:
                break
            url = f'{self.BASE_URL}activities?projectId={self.project_id}&lastId={lastId}'
            response = self.safe_request(url)
            data = response.json()
            if not data:
                break
            data_objects.extend(data)
            if data:
                depth += 1
        return data_objects
    
    def get_views(self):
        url = f'{self.BASE_URL}views?projectId={self.project_id}'
        response = requests.get(url, headers=self.headers)
        return response.json()

    # /projects/{projectId}/users
    def get_project_users(self):
        response = requests.get(
            f"{self.BASE_URL}projects/{self.project_id}/users",
            headers=self.headers,
        )
        data = response.json()
        while response.headers.get("next"):
            response = requests.get(
                response.headers.get("next"),
                headers=self.headers,
            )
            data = data + response.json()
            print(len(data))
        return data
    
    def add_tag(self,tag):
        headers = self.headers | {"Content-Type": "application/json"}
        tag_object = {
            "label": tag,
            "projectId": self.project_id,
        }
        response = requests.post(
            f"{self.BASE_URL}tags",
            headers=headers,
            json=tag_object,
        )
        return response.json()
    
    def get_tags(self):
        url = f'{self.BASE_URL}tags?projectId={self.project_id}&includeDeletedObjects=false'
        response = requests.get(url, headers=self.headers)
        return response.json()
    
    def get_tag(self,tag_id):
        url = f'{self.BASE_URL}tags/{tag_id}'
        response = requests.get(url, headers=self.headers)
        return response.json()
    
    def get_tagged_objects(self,tag_id):
        url = f'{self.BASE_URL}tags/{tag_id}/objects'
        response = requests.get(url, headers=self.headers)
        return response.json()
    
    def delete_tags(self,tag_id):
        url = f'{self.BASE_URL}tags/{tag_id}'
        response = requests.delete(url, headers=self.headers)
        return response
    
    def add_objects_to_tag(self,tag_id,object_list):
        '''
        API to map objects to a tag (objectType: FILE, FOLDER, TOPIC, TODO)
        Example object list 
        object_list = [
                        {
                            "id": "woC32em2Fb0",
                            "objectType": "FILE"
                        },
                        {
                            "id": "woC32em2Fb0",
                            "objectType": "FOLDER"
                        }
                    ]
        '''
        headers = self.headers | {"Content-Type": "application/json"}
        tag_object = object_list
        response = requests.post(
            f"{self.BASE_URL}tags/{tag_id}/objects",
            headers=headers,
            json=tag_object,
        )
        return response.json()

    def get_clashsets(self):
        url = f'{self.BASE_URL}clashsets?projectId={self.project_id}'
        response = requests.get(url, headers=self.headers)
        return response.json()

    def get_clash_details(self,clashsetId):
        '''
        clashsetId: id of clashset
        '''
        url = f'{self.BASE_URL}clashsets/{clashsetId}/items'
        response = requests.get(url, headers=self.headers)
        return response.json()
    
    def delete_clash(self, clashsetId):
        '''
        clashsetId: id of clashset
        '''
        headers = {
            'accept': '*/*',
            'Authorization' : f'Bearer {self.authentication.access_token}',
        }
        url = f'{self.BASE_URL}clashsets/{clashsetId}'
        response = requests.delete(url, headers=headers)
        if response.content:
            return response.json()
        else:
            return response.status_code
    
    def post_clashset(self, name, tolerance, models, ignoreSameDiscipline=False, type_of_clash="CLEARANCE"):
        '''
        Post a clashset: 
        name: name of clashset
        type_of_clash: CLEARANCE (default), CLASH
        tolerance: tolerance in mm (float)
        models: list of model ids
        
        '''
        headers = self.headers | {"Content-Type": "application/json"}
        data = {
            "name" : name,
            "type" : type_of_clash,
            "clearance" : tolerance,
            "ignoreSameDiscipline" : ignoreSameDiscipline,
            "models" : models
        }
        url = '{self.BASE_URL}clashsets'
        response = requests.post(url, headers=headers, json=data)
        return response.json()
    
    def list_all_clash_items(self, clashsetId):
        '''
        clashsetId: id of clashset
        '''
        headers = self.headers
        url = f'{self.BASE_URL}clashsets/{clashsetId}/items'
        response = requests.get(url, headers=headers)
        return response.json()
    
    def get_todos(self):
        headers = self.headers
        url = f"{self.BASE_URL}todos?projectId={self.project_id}"
        response = requests.get(url,headers=headers)
        return response.json()

    def get_todo_attachments(self,todoId):
        headers = self.headers
        url = f"{self.BASE_URL}todos/{todoId}/attachments"
        response = requests.get(url,headers=headers)
        return response.json()
    
    def get_2d_view(self,view_id):
        url = f"{self.BASE_URL}views2d/{view_id}"
        response = requests.get(url,headers=self.headers)
        return response.json()

class TrimbleFile:
    def __init__(self, id, versionId, name, parentId, parentType, fileType, createdTime, modifiedTime, createdBy, modifiedBy, size, md5, revision, thumbnail,checkoutBy=None,checkoutTime=None, full_path=None, parent_folder=None, deleted=None):
        self.id = id
        self.versionId = versionId
        self.name = name
        self.parentId = parentId
        self.parentType = parentType
        self.fileType = fileType
        self.createdTime = createdTime
        self.modifiedTime = modifiedTime
        self.createdBy = createdBy
        self.modifiedBy = modifiedBy
        self.size = size
        self.deleted = deleted
        self.md5 = md5
        self.revision = revision
        self.checkoutBy = checkoutBy
        self.checkoutTime = checkoutTime
        self.thumbnail = thumbnail
        self.full_path = full_path
        self.parent_folder = parent_folder

    def __repr__(self):
        return f"<TrimbleFile {self.name} at {self.full_path}>"

# class ToDo: