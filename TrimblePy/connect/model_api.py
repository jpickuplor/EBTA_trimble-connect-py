import requests
import pandas as pd
import copy
from tqdm import tqdm
import multiprocessing
from multiprocessing import Pool



class ModelApi:

    def __init__(self, authentication):
        self.authentication = authentication
        self.headers = {
            "Authorization": f"Bearer {self.authentication.access_token}",
        }
        self.BASE_URL = self.authentication.endpoints['model']

    def get_model_layers(self, model_id):
        url = f"{self.BASE_URL}models/{model_id}/layers"
        response = requests.get(url, headers=self.headers)
        return response.json()

    def get_model_entities(self, model_id, offset):
        url = f"{self.BASE_URL}models/{model_id}/entities?top=1000&offset={offset}&include=id,idx,psets,psets.name,product,layerIds"
        response = requests.get(url, headers=self.headers)
        return response.json()

    def get_pset_defs(self, model_id):
        url = f"{self.BASE_URL}models/{model_id}/psetdefs"
        response = requests.get(url, headers=self.headers)
        return response.json()

    def get_model_info(self, versionId):
        token = self.authentication.access_token
        if not token:
            raise ValueError("No access token available.")
        url = f"{self.BASE_URL}models/{versionId}?include=metadata"
        for attempt in range(5):
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                return response.json()
            except Exception as err:
                error_message = f"An error occurred for versionId {versionId}: {err}"
                print(error_message)
                if attempt == 4:
                    return None

    def build_df_models(self, versionIds):
        model_data = []
        for versionId in tqdm(versionIds):
            data = self.get_model_info(versionId)
            if data:
                model_data.append(data)
        df_models = pd.DataFrame(model_data)
        df_models.drop(columns=["hierarchyTypes", "metadata"], inplace=True)
        return df_models

    def construct_model(self, df_row):
        model = Model(
            id=df_row.id,  # assuming 'id' is the model's unique ID
            versionId=df_row.versionId,
            name=df_row["name"],
            parentId=df_row.parentId,
            parentType=df_row.parentType,
            fileType=df_row.fileType,
            createdTime=df_row.createdTime,
            modifiedTime=df_row.modifiedTime,
            createdBy=df_row.createdBy,
            modifiedBy=df_row.modifiedBy,
            size=df_row.size,
            deleted=df_row.deleted,
            md5=df_row.md5,
            revision=df_row.revision,
            checkoutBy=df_row.checkoutBy,
            checkoutTime=df_row.checkoutTime,
            thumbnail=df_row.thumbnail,
            full_path=df_row.full_path,
            parent_folder=df_row.parent_folder,
            trbSize=df_row.trbSize,
            ownerCount=df_row.ownerCount,
            historyCount=df_row.historyCount,
            psetDefCount=df_row.psetDefCount,
            hierarchyCount=df_row.hierarchyCount,
            productCount=df_row.productCount,
            entityCount=df_row.entityCount,
            layerCount=df_row.layerCount,
        )
        data_, psetData, layerData = self.get_entity_data(
            df_row.versionId, df_row.entityCount
        )
        entities = self.construct_entities(data_, psetData, layerData, model)
        model.entities = entities
        return model

    @staticmethod
    def _construct_model_worker(args):
        # Unpack arguments to access the instance of ModelApi and the row data
        instance, df_row = args
        return instance.construct_model(df_row)
  
    def construct_models(self, df_rows, n_workers=6):
        # Convert df_rows to a list of tuples where each tuple is arguments for _construct_model_worker
        worker_args = [(self, df_row) for _, df_row in df_rows.iterrows()]
        
        with multiprocessing.Pool(processes=n_workers) as pool:
            # Initiate the pool of workers to process each tuple of arguments
            # models = list(pool.imap_unordered(ModelApi._construct_model_worker, worker_args)) with tqdm
            models = list(tqdm(pool.imap_unordered(ModelApi._construct_model_worker, worker_args), total=len(worker_args)))
        
        return models

    def get_entity_data(self, model_id, entity_count):
        if entity_count < 1000:
            res = self.get_model_entities(model_id, offset=0)
            psetData = self.get_pset_defs(model_id)["items"]
            layerData = self.get_model_layers(model_id)["items"]
            data_ = res["items"]
        else:
            batches = int(entity_count // 1000)
            print("batch count: ", batches)
            data = []
            for i in range(batches):
                print("Batch: ", i)
                res = self.get_model_entities(model_id, offset=int(i * 1000))
                data.append(res)
            remainder = int(entity_count % 1000)
            if remainder > 0:
                print("remainder: ", remainder)
                res = self.get_model_entities(model_id, offset=int(batches * 1000))
                data.append(res)
            data_ = []
            for i in data:
                data_.extend(i["items"])
            psetData = self.get_pset_defs(model_id)["items"]
            layerData = self.get_model_layers(model_id)["items"]
        return data_, psetData, layerData

    def construct_entities(self, entityData, psetData, layerData, model):
        # Convert layer idx to layer name
        layer_idx_to_name = {layer["idx"]: layer["name"] for layer in layerData}

        # Convert pset idx to pset names and property names to values
        pset_idx_to_name = {
            pset["idx"]: {
                "name": pset["name"],
                "properties": {prop["name"]: "" for prop in pset["props"]},
            }
            for pset in psetData
        }

        # Build entities with combined data
        entities = []
        for entity in entityData:
            entity_id = entity["id"]
            idx = entity["idx"]
            ifc_type = entity["type"]
            product = entity["product"]

            # Assign psets (using a deep copy to avoid shared reference issues)
            entity_psets = copy.deepcopy(pset_idx_to_name)
            for pset in entity["psets"]:
                if pset["idx"] in entity_psets:
                    pset_name = entity_psets[pset["idx"]]["name"]
                    for prop_name, prop_value in zip(
                        entity_psets[pset["idx"]]["properties"].keys(), pset["values"]
                    ):
                        entity_psets[pset["idx"]]["properties"][prop_name] = prop_value

            # Simplify the psets data structure to {pset_name: {prop_name: prop_value}}
            simplified_psets = {
                pset_info["name"]: pset_info["properties"]
                for pset_info in entity_psets.values()
            }

            # Assign layer name
            layer_names = [
                layer_idx_to_name[layer_id]
                for layer_id in entity["layerIds"]
                if layer_id in layer_idx_to_name
            ]
            layer = (
                layer_names[0] if layer_names else None
            )  # Assuming there is at most one layer per entity

            # Create Entity object
            ent_obj = Entity(
                entity_id, idx, ifc_type, product, simplified_psets, layer, model=model
            )
            entities.append(ent_obj)

        return entities

    def entities_object(self, entity):
        entity_dict = {
            "entity_id": entity.entity_id,
            "idx": entity.idx,
            "ifc_type": entity.ifc_type,
            "product": entity.product,  # Assuming product is an unpackable structure
            "layer": entity.layer,
            "psets": {pset: props for pset, props in entity.psets.items()},
        }
        return entity_dict

    def entity_to_df(self, entity, include_product=False):
        # Initialize an empty list to hold pset records
        psets_records = []
        # Process psets
        for pset_name, pset_props in entity.psets.items():
            for pset_prop, pset_value in pset_props.items():
                psets_records.append(
                    {
                        "entity_id": entity.entity_id,
                        "pset_name": pset_name,
                        "pset_prop": pset_prop,
                        "pset_value": pset_value,
                    }
                )
        # Convert psets records to a DataFrame
        psets_df = pd.DataFrame(psets_records)
        # Process product and convert to a DataFrame
        product_df = (
            pd.json_normalize(entity.product).add_prefix("product_")
            if include_product
            else pd.DataFrame()
        )
        # Combine DataFrames
        combined_df = psets_df
        if include_product:
            # Add a single row of product information to every pset row
            product_df = pd.concat([product_df] * len(psets_df), ignore_index=True)
            combined_df = pd.concat([combined_df, product_df], axis=1)
        combined_df["idx"] = entity.idx
        combined_df["ifc_type"] = entity.ifc_type
        combined_df["id"] = entity.model.id
        combined_df["versionId"] = entity.model.versionId
        combined_df["model_name"] = entity.model.name
        return combined_df
    
    def entity_to_df_optimized(self, entities, include_product=False):
        # Initialize lists to hold entity and pset records
        entities_records = []
        psets_records = []

        # Process entities
        for entity in entities:
            # Prepare a record for the entity itself
            entity_record = {
                'entity_id': entity.entity_id,
                'idx': entity.idx,
                'ifc_type': entity.ifc_type,
                'layer': entity.layer,
                # Add other entity fields as needed
            }

            # Include product info if requested
            if include_product:
                product_info = pd.json_normalize(entity.product).to_dict(orient='records')[0]
                product_info = {'product_' + k: v for k, v in product_info.items()}
                entity_record.update(product_info)

            entities_records.append(entity_record)

            # Process psets
            for pset_name, pset_props in entity.psets.items():
                for pset_prop, pset_value in pset_props.items():
                    pset_record = {
                        "entity_id": entity.entity_id,
                        "pset_name": pset_name,
                        "pset_prop": pset_prop,
                        "pset_value": pset_value,
                        # Include other entity info if required
                        'idx': entity.idx,
                        'ifc_type': entity.ifc_type
                    }
                    psets_records.append(pset_record)

        # Convert entities list of dicts to a DataFrame
        entities_df = pd.DataFrame(entities_records)
        # Convert psets list of dicts to a DataFrame
        psets_df = pd.DataFrame(psets_records)

        # Combine DataFrames on entity_id to get one row per entity-pset-property value
        combined_df = pd.merge(entities_df, psets_df, on='entity_id', how='outer')
        return combined_df

    def process_entities_with_multiprocessing(self, entities, n_workers=6):
        # Now the process_entity_to_df will be working on the optimized version
        with multiprocessing.Pool(processes=n_workers) as pool:
            # imap_unordered can still be used here
            dfs = pool.map(self.entity_to_df_optimized, [entities[i::n_workers] for i in range(n_workers)])

        # Concatenate all DataFrames together at once
        all_entities_df = pd.concat(dfs, ignore_index=True)
        return all_entities_df



class Entity:
    def __init__(
        self, entity_id, idx, ifc_type, product, psets, layer, model, **kwargs
    ):
        self.entity_id = entity_id
        self.idx = idx
        self.ifc_type = ifc_type
        self.product = product  # This may need unpacking if it contains details
        self.psets = psets
        self.layer = layer  # A single layer name instead of a list of layer_ids
        self.model = model  # Add a reference to the Model instance
        # Additional attributes from **kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        # Representation method now includes the model reference
        return (
            f"<Entity ID: {self.entity_id}, "
            f"Type: {self.ifc_type}, "
            f"Layer: {self.layer}, "
            f"Model: {self.model}, "  # Access the model's data
            # f"Psets: [{', '.join(self.psets.keys())}]>"
        )


class Model:
    def __init__(
        self,
        id,
        versionId,
        name,
        parentId,
        parentType,
        fileType,
        createdTime,
        modifiedTime,
        createdBy,
        modifiedBy,
        size,
        deleted,
        md5,
        revision,
        checkoutBy,
        checkoutTime,
        thumbnail,
        full_path,
        parent_folder,
        trbSize,
        ownerCount,
        historyCount,
        psetDefCount,
        hierarchyCount,
        productCount,
        entityCount,
        layerCount,
        entities=None,
    ):
        self.id = id
        self.versionId = versionId
        self.name = name
        self.parent_id = parentId
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
        self.trb_size = trbSize
        self.ownerCount = ownerCount
        self.historyCount = historyCount
        self.pset_defCount = psetDefCount
        self.hierarchyCount = hierarchyCount
        self.productCount = productCount
        self.entityCount = entityCount
        self.layerCount = layerCount
        self.entities = entities if entities is not None else []

    def add_entity(self, entity_data):
        # Create an Entity object with a backlink to the Model
        new_entity = Entity(model=self, **entity_data)
        self.entities.append(new_entity)

    def __repr__(self):
        return (
            f"<Model ID: {self.id}, "
            f"<Version ID: {self.versionId}, "
            f"Name: {self.name}, "
            f"Number of Entities: {len(self.entities)}>"
        )
