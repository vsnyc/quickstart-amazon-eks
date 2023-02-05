import boto3
import cfnresponse
import json
import logging
from botocore.config import Config
from random import randint
from time import sleep
from uuid import uuid4

logger = logging.getLogger()
SX = "SharedResources"
C = Config(retries={"max_attempts": 10, "mode": "standard"})
BC = boto3.client
CFN = "cloudformation"


def waiter(c, o, s):
    logger.info(f"waiter({o}, {s}) started")
    retries = 50

    while True:
        retries -= 1
        status = c.describe_stacks(StackName=s)["Stacks"][0]["StackStatus"]
        if status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"]:
            break

        if (
            status.endswith("FAILED")
            or status in ["DELETE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"]
            or retries == 0
        ):
            raise RuntimeError(f"Stack operation failed: {o} {status} {s}")

        sleep(randint(1000, 1500) / 100)

    logger.info(f"waiter({o}, {s}) done")


def get_stacks(key, val, region=None):
    cfn = BC(CFN, region_name=region, config=C)
    stacks = []

    for p in cfn.get_paginator("describe_stacks").paginate():
        stacks += p["Stacks"]
    s = [s for s in stacks if {"Key": key, "Value": val} in s["Tags"]]

    if not len(s):
        return None

    stack_id = s[0]["StackId"]
    status = s[0]["StackStatus"]

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

    if name == f"Account{SX}":
        for r in [r["RegionName"] for r in BC("ec2").describe_regions()["Regions"]]:
            acc_stack = get_stacks(key, name, r)
            if acc_stack:
                region = r
                break

    stack_id = get_stacks(key, name, region)
    client = BC(CFN, region_name=region)

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

    method = client.create_stack

    wait = "create"
    if stack_id:
        method = client.update_stack
        wait = "update"
        del args["OnFailure"]

    try:
        stack_id = method(**args)["StackId"]
    except Exception as e:
        if "No updates are to be performed" in str(e):
            return

        logger.exception("Error getting stack ID")
        raise

    waiter(client, wait, stack_id)


def handler(event, context):
    props = event.get("ResourceProperties", None)
    logger.setLevel(props.get("LogLevel", logging.INFO))

    logger.debug(json.dumps(event))

    s = cfnresponse.SUCCESS
    p = event.get("PhysicalResourceId", context.log_stream_name)
    props = event["ResourceProperties"]
    key = props["Key"]
    acc_uri = props["AccountTemplateUri"]
    bucket = acc_uri.split("https://")[1].split(".")[0]
    prefix = "/".join(acc_uri.split("/")[3:-2]) + "/"

    try:
        if event["RequestType"] != "Delete":
            retries = 10
            while True:
                retries -= 1

                try:
                    put_stack(f"Account{SX}", None, acc_uri, {}, key)
                    put_stack(
                        f"Regional{SX}",
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
        s = cfnresponse.FAILED
        logger.exception("Error processing request")
    finally:
        cfnresponse.send(event, context, s, {}, p)
