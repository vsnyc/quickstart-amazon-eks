import json
import logging
import boto3
import subprocess  # nosec B404
import shlex
import time
from hashlib import md5
from crhelper import CfnResource


logger = logging.getLogger(__name__)
helper = CfnResource(json_logging=True, log_level="DEBUG")

try:
    s3_client = boto3.client("s3")
    kms_client = boto3.client("kms")
except Exception as init_exception:
    helper.init_failure(init_exception)


def run_command(command):
    try:
        logger.info(f"executing command: {command}")
        output = subprocess.check_output(  # nosec B603
            shlex.split(command), stderr=subprocess.STDOUT
        ).decode("utf-8")
        logger.info(output)
    except subprocess.CalledProcessError as exc:
        logger.exception(
            "Command failed with exit code %s, stderr: %s"
            % (exc.returncode, exc.output.decode("utf-8"))
        )
        raise Exception(exc.output.decode("utf-8"))

    return output


def create_kubeconfig(cluster_name):
    run_command(
        f"aws eks update-kubeconfig --name {cluster_name} --alias {cluster_name}"
    )
    run_command(f"kubectl config use-context {cluster_name}")


@helper.create
@helper.update
def create_handler(event, _):
    create_kubeconfig(event["ResourceProperties"]["ClusterName"])

    name = event["ResourceProperties"]["Name"]
    retry_timeout = 0

    if "Timeout" in event["ResourceProperties"]:
        retry_timeout = int(event["ResourceProperties"]["Timeout"])

    if retry_timeout > 600:
        retry_timeout = 600

    namespace = event["ResourceProperties"]["Namespace"]
    json_path = event["ResourceProperties"]["JsonPath"]

    while True:
        try:
            outp = run_command(
                f'kubectl get {name} -o jsonpath="{json_path}" --namespace {namespace}'
            )
            break
        except Exception:
            if retry_timeout < 1:
                logger.error("Out of retries")
                raise
            else:
                logger.info("Retrying until timeout...")

                time.sleep(5)
                retry_timeout = retry_timeout - 5

    response_data = {}

    if "ResponseKey" in event["ResourceProperties"]:
        response_data[event["ResourceProperties"]["ResponseKey"]] = outp

    if len(outp.encode("utf-8")) > 1000:
        outp = "MD5-" + str(md5(outp.encode("utf-8")).hexdigest())  # nosec B324

    helper.Data.update(response_data)

    return outp


def handler(event, context):
    props = event.get("ResourceProperties", {})
    logger.setLevel(props.get("LogLevel", logger.INFO))

    logger.debug(json.dumps(event))

    helper(event, context)
