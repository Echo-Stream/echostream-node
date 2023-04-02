from configparser import ConfigParser
from sys import argv

import boto3

echostream_node_zip = None
with open(argv[1], "rb") as f:
    echostream_node_zip = f.read()

config = ConfigParser()
config.read("setup.cfg")
version = config["metadata"]["version"]

layer_name = f'echostream-node-{version.replace(".", "_")}'
for region_name in ("us-east-1", "us-east-2", "us-west-1", "us-west-2"):
    print(f"Publishing {layer_name} to {region_name}")
    lambda_client = boto3.client("lambda", region_name=region_name)
    response = lambda_client.publish_layer_version(
        CompatibleArchitectures=["x86_64"],
        CompatibleRuntimes=["python3.9"],
        Content=dict(ZipFile=echostream_node_zip),
        Description=f"echostream-node=={version} with all dependencies not present in the AWS Lambda environment",
        LayerName=layer_name,
        LicenseInfo="APL2",
    )
    lambda_client.add_layer_version_permission(
        Action="lambda:GetLayerVersion",
        LayerName=response["LayerArn"],
        Principal="*",
        StatementId="PublicAccess",
        VersionNumber=response["Version"],
    )
