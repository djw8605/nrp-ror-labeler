import argparse
import csv
import re
import unicodedata
from urllib.parse import urlparse

from kubernetes import client, config


# add arguments
def add_args(parser):
    parser.add_argument("--add-missing", action="store_true", help="Add missing nodes to the csv file")
    parser.add_argument("table", help="The CSV file containing the hostname and ROR values of the nodes")
    return parser.parse_args()


def convert_url_to_k8s_value(url):
    # The ror is a URL, parse it
    if not url:
        return None
    parsed = urlparse(url)
    # Ensure hostname exists; combine hostname and path, replacing slashes with underscores
    hostname = parsed.hostname or ""
    path = parsed.path.replace("/", "_") if parsed.path else ""
    value = f"{hostname}{path}"
    return sanitize_k8s_label_value(value)


def sanitize_k8s_label_value(value):
    if value is None:
        return None

    # Remove accents/symbol variants (e.g., en-dash) so values stay label-safe.
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = value.strip()
    if not value:
        return None

    # K8s label values allow only alnum plus '-', '_' and '.'
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    value = re.sub(r"^[^A-Za-z0-9]+", "", value)
    value = re.sub(r"[^A-Za-z0-9]+$", "", value)
    if not value:
        return None

    if len(value) > 63:
        value = re.sub(r"[^A-Za-z0-9]+$", "", value[:63])

    return value if value else None


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

        # Compute desired label values from CSV (prefer OSG Value for OSG label, fallback to ROR)
        csv_row = node_institution[node.metadata.name]
        osg_val_raw = (csv_row.get('OSG Value') or "").strip()
        ror_val_raw = (csv_row.get('ROR Value') or "").strip()
        institution_raw = (csv_row.get('Institution Name') or "").strip()

        desired_osg = convert_url_to_k8s_value(osg_val_raw) if osg_val_raw else (convert_url_to_k8s_value(ror_val_raw) if ror_val_raw else None)
        desired_ror = convert_url_to_k8s_value(ror_val_raw) if ror_val_raw else None
        desired_institution = sanitize_k8s_label_value(institution_raw) if institution_raw else None

        # Current labels on the node
        current_labels = node.metadata.labels or {}
        current_osg = current_labels.get("nautilus.io/OSGInstitutionID")
        current_ror = current_labels.get("nautilus.io/RORInstitutionID")
        current_institution = current_labels.get("nautilus.io/Institution")

        labels_to_patch = {}

        # Only set OSG label if desired value exists and differs from current
        if desired_osg is not None and desired_osg != current_osg:
            labels_to_patch["nautilus.io/OSGInstitutionID"] = desired_osg

        # Only set ROR label if desired value exists and differs from current
        if desired_ror is not None and desired_ror != current_ror:
            labels_to_patch["nautilus.io/RORInstitutionID"] = desired_ror

        # Only set Institution label if desired value exists and differs from current
        if desired_institution is not None and desired_institution != current_institution:
            labels_to_patch["nautilus.io/Institution"] = desired_institution

        if not labels_to_patch:
            print(f"Node {node.metadata.name}: labels are up-to-date, skipping patch")
            continue

        body = {
            "metadata": {
                "labels": labels_to_patch
            }
        }

        print(f"Patching node {node.metadata.name} with {labels_to_patch}")
        v1.patch_node(node.metadata.name, body)

        #api_response = api_instance.patch_node(node.metadata.name, body)
        #print(f"{node.metadata.name}\t{node.metadata.labels}")


if __name__ == "__main__":
    main()
