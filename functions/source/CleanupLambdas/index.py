import boto3
import json
import logging
from crhelper import CfnResource # Provided through CrhelperLayer in amazon-eks-per-region-resources.template.yaml

logger = logging.getLogger(__name__)
helper = CfnResource(json_logging=True, log_level="DEBUG")
lmbd = boto3.client("lambda")


@helper.delete
def delete_handler(event, _):
    security_group_id = event["ResourceProperties"]["SecurityGroupId"]
    paginator = lmbd.get_paginator("list_functions")

    for page in paginator.paginate():
        for func in page["Functions"]:
            if security_group_id not in func.get("VpcConfig", {}).get(
                "SecurityGroupIds", []
            ):
                continue

            logger.info(f"deleting {func['FunctionName']}")

            lmbd.delete_function(FunctionName=func["FunctionName"])


def handler(event, context):
    props = event.get("ResourceProperties", {})
    logger.setLevel(props.get("LogLevel", logging.INFO))

    logger.debug(json.dumps(event))

    helper(event, context)
