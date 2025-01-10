import argparse
import csv
from urllib.parse import urlparse

from kubernetes import client, config


# add arguments
def add_args(parser):
    parser.add_argument("--add-missing", action="store_true", help="Add missing nodes to the csv file")
    parser.add_argument("table", help="The CSV file containing the hostname and ROR values of the nodes")
    return parser.parse_args()


def convert_url_to_k8s_value(url):
    # The ror is a URL, parse it
    parsed = urlparse(url)

    return f'{parsed.hostname}{parsed.path.replace("/", "_")}'


def main():
    parser = argparse.ArgumentParser()
    args = add_args(parser)

    # Load the kubernetes configuration file
    config.load_kube_config(context="nautilus")
    v1 = client.CoreV1Api()
    node_list = v1.list_node()
    #print(node_list)

    # Create a new dict of the node names and their institutions
    node_institution = {}
    with open(args.table, 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        for row in reader:
            node_institution[row['OSG Identifier']] = row
            #print(row)

    # Loop through the list of nodes in node_list
    for node in node_list.items:
        if node.metadata.name not in node_institution:
            if args.add_missing:
                print(f"{node.metadata.name}")
                #node_institution[node.metadata.name] = "None"
            else:
                print(f"Node {node.metadata.name} not found in the csv file")
            continue

        body = {
            "metadata": {
                "labels": {
                    "OSGInstitutionID": None,
                    "RORInstitutionID": None,
                    "nautilus.io/OSGInstitutionID": convert_url_to_k8s_value(node_institution[node.metadata.name]['OSG Value']) if node_institution[node.metadata.name]['OSG Value'] else convert_url_to_k8s_value(node_institution[node.metadata.name]['ROR Value']),
                    "nautilus.io/RORInstitutionID": convert_url_to_k8s_value(node_institution[node.metadata.name]['ROR Value']),
                }
            }
        }
        #print(f"{node.metadata.name}\t{body}")
        print(f"Patching node {node.metadata.name}")
        v1.patch_node(node.metadata.name, body)

        #api_response = api_instance.patch_node(node.metadata.name, body)
        #print(f"{node.metadata.name}\t{node.metadata.labels}")


if __name__ == "__main__":
    main()
