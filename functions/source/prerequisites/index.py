import boto3
import cfnresponse
import json
import logging
from botocore.config import Config
from random import randint
from time import sleep
from uuid import uuid4

logger = logging.getLogger()
client = boto3.client
regions = client("ec2").describe_regions()["Regions"]


def waiter(cfn, operation, stack_id):
    logger.info(f"waiter({operation}, {stack_id}) started")
    retries = 50

    while True:
        retries -= 1
        status = cfn.describe_stacks(StackName=stack_id)["Stacks"][0]["StackStatus"]

        if status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"]:
            break

        if (
            status.endswith("FAILED")
            or status in ["DELETE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"]
            or retries == 0
        ):
            raise RuntimeError(
                f"Stack operation failed: {operation} {status} {stack_id}"
            )

        sleep(randint(1000, 1500) / 100)

    logger.info(f"waiter({operation}, {stack_id}) done")


def get_stacks(key, value, region=None):
    c = Config(retries={"max_attempts": 10, "mode": "standard"})
    cfn = client("cloudformation", region_name=region, config=c)
    stacks = []

    for page in cfn.get_paginator("describe_stacks").paginate():
        stacks += page["Stacks"]
    stack = [stack for stack in stacks if {"Key": key, "Value": value} in stack["Tags"]]

    if not len(stack):
        return None

    stack_id = stack[0]["StackId"]
    status = stack[0]["StackStatus"]

    if status.endswith("_IN_PROGRESS"):
        op = status.split("_")[0].lower()

        waiter(cfn, op, stack_id)

        if op == "delete":
            return None

    return stack_id


def put_stack(name, region, template_url, parameters, key):
    logger.info(f"put_stack({name}, {region}, {template_url}, {parameters}, {key})")

    # jitter to reduce the chance of concurrent queries racing
    sleep(randint(0, 6000) / 100)

    if name == "AccountSharedResources":
        for r in [r["RegionName"] for r in regions]:
            account_stack = get_stacks(key, name, r)
            if account_stack:
                region = r
                break

    stack_id = get_stacks(key, name, region)
    cfn = client("cloudformation", region_name=region)

    args = {
        "StackName": stack_id if stack_id else f"{key}-{name}",
        "TemplateURL": template_url,
        "Parameters": [
            {"ParameterKey": k, "ParameterValue": v} for k, v in parameters.items()
        ],
        "Capabilities": [
            "CAPABILITY_IAM",
            "CAPABILITY_NAMED_IAM",
            "CAPABILITY_AUTO_EXPAND",
        ],
        "OnFailure": "DELETE",
        "Tags": [{"Key": key, "Value": name}],
    }

    method = cfn.create_stack

    wait = "create"
    if stack_id:
        method = cfn.update_stack
        wait = "update"
        del args["OnFailure"]

    try:
        stack_id = method(**args)["StackId"]
    except Exception as e:
        if "No updates are to be performed" in str(e):
            return

        logger.exception("Error getting stack ID")
        raise

    waiter(cfn, wait, stack_id)


def handler(event, context):
    logger.debug(json.dumps(event))

    responseStatus = cfnresponse.SUCCESS
    physicalResourceId = event.get("PhysicalResourceId", context.log_stream_name)
    props = event["ResourceProperties"]
    key = props["Key"]
    account_template_uri = props["AccountTemplateUri"]
    bucket = account_template_uri.split("https://")[1].split(".")[0]
    prefix = "/".join(account_template_uri.split("/")[3:-2]) + "/"

    try:
        if event["RequestType"] != "Delete":
            retries = 10
            while True:
                retries -= 1

                try:
                    put_stack(
                        "AccountSharedResources", None, account_template_uri, {}, key
                    )
                    put_stack(
                        "RegionalSharedResources",
                        None,
                        props["RegionalTemplateUri"],
                        {
                            "QSS3BucketName": bucket,
                            "QSS3KeyPrefix": prefix,
                            "RandomStr": uuid4().hex,
                        },
                        key,
                    )
                    break
                except Exception:
                    logger.exception("Error executing put_stack")

                    if retries > 0:
                        sleep(randint(0, 3000) / 100)
                    else:
                        raise
    except Exception:
        responseStatus = cfnresponse.FAILED
        logger.exception("Error processing request")
    finally:
        cfnresponse.send(event, context, responseStatus, {}, physicalResourceId)
