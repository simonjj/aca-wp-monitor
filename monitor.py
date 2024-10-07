#!/usr/bin/env python

from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient

from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.loganalytics.models import Workspace
from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.monitor.models import MetricAlertResource, MetricAlertCriteria, MetricAlertSingleResourceMultipleMetricCriteria

import requests
import pandas as pd
import json
from datetime import datetime, timezone
from copy import deepcopy
import os
import sys

CLEANUP_TABLE = str.maketrans({
    " ": "",  # Remove spaces
    "(": "",  # Remove opening parenthesis
    ")": ""   # Remove closing parenthesis
})


def _convert_to_float(string_number):
    return float(string_number.replace("Gi", ""))


def _get_workload_profile_types(filename):
    # Load the JSON data from the file
    with open('aca-workload-profiles-definition.json', 'r') as file:
        data = json.load(file)
    # Convert the JSON data into a pandas DataFrame
    return pd.DataFrame(data)


def _get_aca_client(subscription_id):
    # Authenticate using DefaultAzureCredential
    credential = DefaultAzureCredential()
    # Initialize the ContainerAppsAPIClient
    return ContainerAppsAPIClient(credential=credential, subscription_id=subscription_id)


def _get_current_app_replica_count(aca_client, resource_group, app):
    count = 0
    try:
        replicas = aca_client.container_apps_revision_replicas.list_replicas(resource_group, app.name, app.latest_ready_revision_name)
        for replica in replicas.value:
            if replica.running_state == "Running":
                count += 1
    except ValueError as e:
        pass
    return count

# given a workload profile row we evaluate if it is underprovisioned
def _is_wp_underprovisioned(row):
    provisioning_assessment = ""
    if row["Workload Profile"] == "Consumption":
        return provisioning_assessment
    if row["Max Available CPU"] < row["Max Scale Needed CPU"]:
        provisioning_assessment += "CPU underprovisioned by " + str(row["Max Scale Needed CPU"] - row["Max Available CPU"]) + " vCPUs. "

    if row["Max Available Memory (GB)"] < row["Max Scale Needed Memory (GB)"]:
        provisioning_assessment += "Memory underprovisioned by " + str(row["Max Scale Needed Memory (GB)"] - row["Max Available Memory (GB)"]) + " GB."
        row["Currently Available Memory (GB)"] < row["Max Scale Needed Memory (GB)"]
    return provisioning_assessment


# we have to assemble a URL for the metrics API
# format is https://<location>.monitoring.azure.com<resourceId>/metrics
# we use this job (ideally) or app assumed to be running on ACA as a target resource
def _determine_azure_monitor_metrics_url(aca_client, resource_group_name, environment_name, current_app_name):
    # check if we're running as a job
    me_app = None
    try:
        me_app = aca_client.jobs.get(resource_group_name, current_app_name)
    # this script isn't running as a job so let's look for it in apps
    except ResourceNotFoundError:
        me_app = aca_client.container_apps.get(resource_group_name, current_app_name)

    # if we're still unable to find the app we may lack Reader persmissions on the sub
    if not me_app:
        print(f"ERROR: Could not retrieve app data for {current_app_name}, please check app roles and permissions.")
        sys.exit(1)
    location = me_app.location.translate(CLEANUP_TABLE).lower()
    return f"https://{location}.monitoring.azure.com{me_app.id}/metrics"


# read all the container apps for an environment
# for each container app get the currently allocated resources, the workload profile, and the scale min and max settings
# calculate the current, minimum and maximum resources allocated for the environment (all apps)
def get_container_apps_resources(aca_client, resource_group_name, environment_name):
    # List all container apps in the specified environment
    container_apps = aca_client.container_apps.list_by_resource_group(resource_group_name)

    # Initialize lists to store the data
    app_names = []
    workload_profiles = []
    currently_allocated_cpus = []
    currently_allocated_memory_gb = []
    current_replica_counts = []
    min_cpus = []
    min_memory_gb = []
    max_cpus = []
    max_memory_gb = []
    min_replicas = []
    max_replicas = []
    single_replica_cpus = []
    single_replica_memory_gb = []

    # Iterate through each container app
    for app in container_apps:
        # we don't have a per-env call so we filter here
        if app.managed_environment_id.split("/")[-1].lower() != environment_name.lower():
            continue
        app_names.append(app.name)
        workload_profiles.append(app.workload_profile_name)

        # Initialize current, min, and max resources
        single_replica_cpu = 0
        single_replica_memory = 0
        current_allocated_cpu = 0
        current_allocated_memory = 0
        min_cpu = 0
        min_memory = 0
        max_cpu = 0
        max_memory = 0
        max_replica = 0
        min_replica = 0

        # Get the resources needed for a single replica of the application
        if app.template and app.template.containers:
            for container in app.template.containers:
                if container.resources:
                    single_replica_cpu += container.resources.cpu
                    single_replica_memory += _convert_to_float(container.resources.memory)

        # Get the currently allocated cpu and memory for the application
        current_replica_count = _get_current_app_replica_count(aca_client, resource_group_name, app)        
        current_allocated_cpu = single_replica_cpu * current_replica_count
        current_allocated_memory = single_replica_memory * current_replica_count

        # Get the scale settings for the application
        if app.template and app.template.scale:
            min_replica = app.template.scale.min_replicas
            max_replica = app.template.scale.max_replicas
            if min_replica is None:
                min_replica = 0

            min_cpu = single_replica_cpu * min_replica
            min_memory = single_replica_memory * min_replica
            max_cpu = single_replica_cpu * max_replica
            max_memory = single_replica_memory * max_replica

        # Append the resources to the lists
        currently_allocated_cpus.append(current_allocated_cpu)
        currently_allocated_memory_gb.append(current_allocated_memory)
        current_replica_counts.append(current_replica_count) 
        min_cpus.append(min_cpu)
        min_memory_gb.append(min_memory)
        max_cpus.append(max_cpu)
        max_memory_gb.append(max_memory)
        single_replica_cpus.append(single_replica_cpu)
        single_replica_memory_gb.append(single_replica_memory)
        min_replicas.append(min_replica)
        max_replicas.append(max_replica)

    # Create a pandas DataFrame
    df = pd.DataFrame({
        'App Name': app_names,
        'Workload Profile': workload_profiles,
        'Single Replica CPU': single_replica_cpus,
        'Single Replica Memory (GB)': single_replica_memory_gb,
        'Currently Used CPU': currently_allocated_cpus,
        'Currently Used Memory (GB)': currently_allocated_memory_gb,
        'Current Replica Count': current_replica_counts,
        'Min Replicas': min_replicas,
        'Max Replicas': max_replicas,
        'Min Scale Needed CPU': min_cpus,
        'Max Scale Needed CPU': max_cpus,
        'Min Scale Needed Memory (GB)': min_memory_gb,
        'Max Scale Needed Memory (GB)': max_memory_gb
    })
    return df


# read all the workload profiles for an environment
# for reach workload profile get the node type, the current node count, the min node count and the max count for the nodes
# calculate the current, minimum and maximum resources allocated for the environment (all profiles)
def get_workload_profiles_resources(aca_client, resource_group_name, environment_name):
    # List all workload profiles in the specified environment
    workload_profiles = aca_client.managed_environments.list_workload_profile_states(resource_group_name, environment_name)
    wpdf = _get_workload_profile_types('aca-workload-profiles-definition.json')

    # Initialize lists to store the data
    profile_names = []
    node_types = []
    current_node_counts = []
    min_node_counts = []
    max_node_counts = []
    current_cpus = []
    current_memory_gb = []
    min_cpus = []
    min_memory_gb = []
    max_cpus = []
    max_memory_gb = []

    # Iterate through each workload profile
    for profile in workload_profiles:
        # we skip the consumption profile
        if profile.type == "Consumption": 
            continue
        profile_names.append(profile.name)
        node_types.append(profile.type)
        
        # we need these a few times so we keep them for easy access
        cur_nodes = profile.properties.current_count
        min_nodes = profile.properties.minimum_count
        max_nodes = profile.properties.maximum_count

        current_node_counts.append(cur_nodes)
        min_node_counts.append(min_nodes)
        max_node_counts.append(max_nodes)

        # Calculate current, min, and max resources
        # we start by getting the node type and the resources for that node type
        node_cpu, node_memory = wpdf[wpdf["type"] == profile.type][["vCPUs", "memoryGB"]].values[0]

        current_cpu = cur_nodes * node_cpu
        current_memory = cur_nodes * node_memory
        min_cpu = min_nodes * node_cpu
        min_memory = min_nodes * node_memory
        max_cpu = max_nodes * node_cpu
        max_memory = max_nodes * node_memory

        # Append the resources to the lists
        current_cpus.append(current_cpu)
        current_memory_gb.append(current_memory)
        min_cpus.append(min_cpu)
        min_memory_gb.append(min_memory)
        max_cpus.append(max_cpu)
        max_memory_gb.append(max_memory)
    
    # Create a pandas DataFrame
    df = pd.DataFrame({
        'Profile Name': profile_names,
        'Node Type': node_types,
        'Current Node Count': current_node_counts,
        'Min Node Count': min_node_counts,
        'Max Node Count': max_node_counts,
        'Currently Available CPU': current_cpus,
        'Currently Available Memory (GB)': current_memory_gb,
        'Min CPU': min_cpus,
        'Min Memory (GB)': min_memory_gb,
        'Max Available CPU': max_cpus,
        'Max Available Memory (GB)': max_memory_gb
    })
    return df


# we compare the resources allocated to the workload profiles with the resources allocated to the apps
def asses_wp_resources_vs_app_resources(appdf, wpdf):
    apps_summary = appdf.groupby("Workload Profile")[["Currently Used CPU", "Currently Used Memory (GB)", "Max Scale Needed CPU", "Max Scale Needed Memory (GB)"]].sum().reset_index()
    overall_summary = pd.merge(wpdf, apps_summary, right_on='Workload Profile', left_on='Profile Name', how="right")
    # we trim down the full dataframe to only include the columns which we absolutely need and also fill * Available * columns with -1 for in the case of Consumption profile
    overall_summary = overall_summary[["Workload Profile", 
                                       "Currently Used CPU", "Currently Used Memory (GB)", 
                                       "Currently Available CPU", "Currently Available Memory (GB)", 
                                       "Max Scale Needed CPU", "Max Scale Needed Memory (GB)",
                                       "Max Available CPU", "Max Available Memory (GB)"]].fillna(-1)
    # evaluate if the workload profile is underprovisioned
    overall_summary["Provisioning Assessment"] = overall_summary.apply(_is_wp_underprovisioned, axis=1)
    return overall_summary


def log_provisioning_assessment(adf):
    for i, row in adf.iterrows():
        if row["Provisioning Assessment"]:
            print(f"WARNING: Workload Profile {row['Workload Profile']} is underprovisioned: {row['Provisioning Assessment']}")


# we prepare the data for Azure Monitor and use the custom metrics format
# full details here: https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/metrics-store-custom-rest-api?tabs=SDK
def prep_data_for_azure_monitor(sdf):
    metric_stub = {
                    "time": "TBD",
                    "data": {
                        "baseData": {
                            "metric": "TBD",
                            "namespace": "Workload Profile",
                            "dimNames": [
                                "Workload Profile",
                            ],
                            "series": []
                        }
                    }
                 }
    all_metrics = []
    metric_batches = ["Currently Used CPU", "Currently Used Memory (GB)", 
                      "Currently Available CPU", "Currently Available Memory (GB)", 
                      "Max Scale Needed CPU", "Max Scale Needed Memory (GB)",
                      "Max Available CPU", "Max Available Memory (GB)"]

    for batch in metric_batches:
        current_metric = deepcopy(metric_stub)
        current_metric["time"] = datetime.now(timezone.utc).isoformat()
        current_metric["data"]["baseData"]["metric"] = batch
        batch_data = sdf[["Workload Profile", batch]]
        for i, row in batch_data.iterrows():
            current_metric["data"]["baseData"]["series"].append({"dimValues": [row["Workload Profile"]], "sum": row[batch], "count": 1, "min": row[batch], "max": row[batch]})
        all_metrics.append(current_metric)
    return all_metrics
        
        

def send_metrics_to_azure_monitor(metric_data, url):
    # Authenticate using DefaultAzureCredential
    credential = DefaultAzureCredential()
    # Get the token
    token = credential.get_token('https://monitoring.azure.com/.default').token
    # Set the headers
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    # Send the metrics
    for metric in metric_data:
        res = requests.post(url, headers=headers, data=json.dumps(metric))
        if res.status_code != 200:
            print(f"Failed to send metric {metric['data']['baseData']['metric']} with status code {res.status_code} and response {res.text}")
    return res.status_code


# we assume SUBSCRIPTION_ID, RESOURCE_GROUP and ENVIRONMENT_NAME are set in the environment
def main(subscription_id=None, resource_group=None, environment_name=None, current_app_name=None):
    if not subscription_id or not resource_group or not environment_name or not current_app_name:
        subscription_id = os.getenv("SUBSCRIPTION_ID", None)
        resource_group = os.getenv("RESOURCE_GROUP", None)
        environment_name = os.getenv("ENVIRONMENT_NAME", None)
        current_app_name = os.getenv("CONTAINER_APP_JOB_NAME", None)
        if not current_app_name:
            current_app_name = os.getenv("CURRENT_APP_NAME", None)

    if not subscription_id or not resource_group or not environment_name or not current_app_name:
        print("Please set SUBSCRIPTION_ID, RESOURCE_GROUP, ENVIRONMENT_NAME and CONTAINER_APP_JOB_NAME in the environment.")
        return sys.exit(1)

    # connect and gather the data from ACA (data is in pandas DataFrame format)
    aca_client = _get_aca_client(subscription_id)
    appdf = get_container_apps_resources(aca_client, resource_group, environment_name)
    wpdf = get_workload_profiles_resources(aca_client, resource_group, environment_name)
    summary_df = asses_wp_resources_vs_app_resources(appdf, wpdf)

    # log the assessment
    log_provisioning_assessment(summary_df)

    monitor_data = prep_data_for_azure_monitor(summary_df)
    url = _determine_azure_monitor_metrics_url(aca_client, resource_group, environment_name, current_app_name)
    if url:
        send_metrics_to_azure_monitor(monitor_data, url)



if __name__ == "__main__":
    main()
