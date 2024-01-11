import requests
from multiprocessing import Pool


class OrgApi:

    def __init__(self, authentication, project_id=None):
        self.authentication = authentication
        self.headers = {
            "Authorization": f"Bearer {self.authentication.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.BASE_URL = self.authentication.endpoints['org']
        self.project_id = project_id

    def get_discovery_trees(self,forestId):
        response = requests.get(
            f"{self.BASE_URL}forests/{forestId}/trees",
            headers=self.headers,
        )
        return response.json()

    def get_discovery_tree(self,forestId,treeId):
        response = requests.get(
            f"{self.BASE_URL}forests/{forestId}/trees/{treeId}",
            headers=self.headers,
        )
        return response.json()
    
    def get_nodes(self,forestId,treeId):
        response = requests.get(
            f"{self.BASE_URL}forests/{forestId}/trees/{treeId}/nodes",
            headers=self.headers,
        )
        return response.json()

    def get_node(self, forestId, treeId, nodeId):
        response = requests.get(
            f"{self.BASE_URL}forests/{forestId}/trees/{treeId}/nodes/{nodeId}",
            headers=self.headers,
        )
        return response.json()
    