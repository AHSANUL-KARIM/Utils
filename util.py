from datetime import datetime
import json
import os
import requests
import base64

from notebookutils import mssparkutils

class Utils:

    # --- Synapse Auth Utils ---

    def get_access_token(azure_client_id, azure_tenant_id, azure_client_secret):
        # Use the v2.0 endpoint for better cross-tenant support
        url = f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/v2.0/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": azure_client_id,
            "client_secret": azure_client_secret,
            "scope": "https://dev.azuresynapse.net/.default"
        }

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        response = requests.post(url, data=payload, headers=headers)
        
        if not response.ok:
            raise RuntimeError(f"Failed to get token: {response.status_code} - {response.text}")
            
        response_json = response.json()
        return response_json["access_token"]

    # --- Notebook Transformation Utils ---

    def clean_notebook_cells(ntbk_json, tags_to_clean):
        if 'cells' in ntbk_json:
            for cell in ntbk_json['cells']:
                for tag in tags_to_clean:
                    if tag in cell:
                        cell[tag] = []
        return ntbk_json

    # --- Export Logic (Synapse to Fabric Lakehouse/Files) ---

    def export_notebooks(azure_client_id, azure_tenant_id, azure_client_secret, synapse_workspace_name, output_folder):
        Utils.export_resources("notebooks", azure_client_id, azure_tenant_id, azure_client_secret, synapse_workspace_name, output_folder)

    def export_sjd(azure_client_id, azure_tenant_id, azure_client_secret, synapse_workspace_name, output_folder):
        Utils.export_resources("sparkJobDefinitions", azure_client_id, azure_tenant_id, azure_client_secret, synapse_workspace_name, output_folder)

    def export_resources(resource_type, azure_client_id, azure_tenant_id, azure_client_secret, synapse_workspace_name, output_folder):
        base_uri = f"{synapse_workspace_name}.dev.azuresynapse.net"
        api_version = "2020-12-01"
        
        # Get Token
        synapse_dev_token = Utils.get_access_token(azure_client_id, azure_tenant_id, azure_client_secret)
        
        headers = {
            'Authorization': f'Bearer {synapse_dev_token}',
            'Content-Type': 'application/json'
        }

        # List all resources
        url = f"https://{base_uri}/{resource_type}?api-version={api_version}"
        response = requests.get(url, headers=headers)

        if not response.ok:
            raise RuntimeError(f"Exporting '{resource_type}' failed list step: {response.status_code}: {response.text}")

        response_json = response.json()
        items = response_json.get('value', response_json.get('items', []))
        
        print(f"Exporting {len(items)} items of type '{resource_type}' from '{synapse_workspace_name}'...")
        res_exported = 0

        for artifact in items:
            resource_name = artifact.get("name", artifact.get("Name"))
            print(f"  >> Exporting '{resource_name}' ...")
            
            # Fetch individual item detail
            resource_url = f"https://{base_uri}/{resource_type}/{resource_name}?api-version={api_version}"
            res_detail = requests.get(resource_url, headers=headers)

            if res_detail.ok:
                detail_json = res_detail.json()

                if resource_type == "sparkJobDefinitions":
                    file_name = f"{resource_name}.json"
                    data = json.dumps(detail_json, indent=4)
                elif resource_type == "notebooks":
                    # Synapse stores the actual notebook content under the 'properties' key
                    notebook_content = detail_json.get('properties', detail_json)
                    tags_to_clean = ['outputs']
                    updated_json = Utils.clean_notebook_cells(notebook_content, tags_to_clean)
                    file_name = f"{resource_name}.ipynb"
                    data = json.dumps(updated_json, indent=4)
                
                # Save to Fabric Lakehouse Files
                mssparkutils.fs.put(f"{output_folder}/{resource_type}/{file_name}", data, True)
                res_exported += 1
            else:
                print(f"  !! Failed to export '{resource_name}': {res_detail.status_code}")

        print(f"Finished: Exported {res_exported} items of type: {resource_type}")

    # --- Import Logic (Files to Fabric Workspace) ---

    def import_notebooks(output_folder, workspace_id, prefix, notebook_names=None):
        artifact_path = f"{output_folder}/notebooks"
        if not os.path.exists(artifact_path):
            # Check if it's an ABFS path (mssparkutils check)
            try: mssparkutils.fs.ls(artifact_path)
            except: 
                print(f"Path {artifact_path} not found."); return

        if notebook_names is None:
            # Note: os.listdir only works on local mounted drives; 
            # if using ABFS directly, you'd use mssparkutils.fs.ls
            notebook_names = [f.name.split('.')[0] for f in mssparkutils.fs.ls(artifact_path) if f.name.endswith(".ipynb")]

        for name in notebook_names:
            full_path = f"{artifact_path}/{name}.ipynb"
            ntbk_json = json.loads(mssparkutils.fs.head(full_path, 1024*1024*10)) # Load up to 10MB
            Utils.import_notebook(f"{prefix}_{name}", ntbk_json, workspace_id)

    def import_notebook(ntbk_name, ntbk_json, workspace_id):
        api_endpoint = "api.fabric.microsoft.com"
        pbi_token = mssparkutils.credentials.getToken('https://analysis.windows.net/powerbi/api') 

        url = f"https://{api_endpoint}/v1/workspaces/{workspace_id}/items"

        # Encode the notebook for Fabric API
        json_bytes = json.dumps(ntbk_json).encode('utf-8')
        base64_str = base64.b64encode(json_bytes).decode('utf-8')

        payload = json.dumps({
            "type": "Notebook",
            "displayName": ntbk_name,
            "definition" : {
                "format": "ipynb",
                "parts" : [{"path": "notebook-content.ipynb", "payload": base64_str, "payloadType": "InlineBase64"}]
            }
        })

        headers = {'Authorization': f'Bearer {pbi_token}', 'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=payload)

        if response.ok: print(f">> Notebook '{ntbk_name}' created.")
        else: print(f"!! Failed '{ntbk_name}': {response.text}")

    # --- SJD Logic ---

    def import_sjds(output_folder, workspace_id, lakehouse_id, prefix):
        artifact_path = f"{output_folder}/sparkJobDefinitions"
        items = mssparkutils.fs.ls(artifact_path)
        for item in items:
            if item.name.endswith(".json"):
                sjd_json = json.loads(mssparkutils.fs.head(item.path, 1024*1024))
                name = item.name.split('.')[0]
                Utils.import_sjd_from_json(f"{prefix}_{name}", sjd_json, workspace_id, lakehouse_id)

    def import_sjd_from_json(sjd_name, sjd_json, workspace_id, lakehouse_id):
        props = sjd_json.get("properties", {}).get("jobProperties", {})
        
        workload_json = {
            "executableFile": props.get("file", ""),
            "defaultLakehouseArtifactId": lakehouse_id,
            "mainClass": props.get("className", ""),
            "additionalLakehouseIds": [],
            "commandLineArguments": " ".join(props.get("args", [])),
            "additionalLibraryUris": " ".join(props.get("jars", [])),
            "language": sjd_json.get("properties", {}).get("language", "python")
        }
    
        Utils.import_sjd_api(sjd_name, workload_json, workspace_id)

    def import_sjd_api(sjd_name, workload_json, workspace_id):
        api_endpoint = "api.fabric.microsoft.com"
        pbi_token = mssparkutils.credentials.getToken('https://analysis.windows.net/powerbi/api') 

        url = f"https://{api_endpoint}/v1/workspaces/{workspace_id}/items"
        base64_str = base64.b64encode(json.dumps(workload_json).encode('utf-8')).decode('utf-8')

        payload = json.dumps({
            "type": "SparkJobDefinition",
            "displayName": sjd_name,
            "definition" : {
                "format": "SparkJobDefinitionV1",
                "parts" : [{"path": "SparkJobDefinitionV1.json", "payload": base64_str, "payloadType": "InlineBase64"}]
            }
        })

        headers = {'Authorization': f'Bearer {pbi_token}', 'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=payload)
        if response.ok: print(f">> SJD '{sjd_name}' created.")
        else: print(f"!! Failed '{sjd_name}': {response.text}")
