#!/bin/bash

REGION=$1
STACK_ID=$2
FUNCTIONS="eks-quickstart- awsqs-kubernetes-resource-proxy- EKS-QuickStart-Kube QuickStart-ParameterResolver"
TYPE_LOG_GROUPS="/cloudformation/registry/awsqs-kubernetes-helm /cloudformation/registry/awsqs-kubernetes-get /cloudformation/registry/awsqs-kubernetes-resource /cloudformation/registry/awsqs-eks-cluster"

if [ -z $REGION ] || [ -z $STACK_ID ]; then
  echo "usage: ./gather-logs.sh REGION STACK_ID"
  exit 1
fi

function gather_stack(){
  mkdir -p /tmp/eks-qs-logs/
  echo "gathering logs for CloudFormation stack $1..."
  n=$(echo $1 | awk -F/ '{print $2}')
  aws cloudformation describe-stacks --stack-name $1 --region $REGION > /tmp/eks-qs-logs/${n}-describe-stack.json
  aws cloudformation describe-stack-events --stack-name $1 --region $REGION > /tmp/eks-qs-logs/${n}-describe-stack-events.json
  aws cloudformation describe-stack-resources --stack-name $1 --region $REGION > /tmp/eks-qs-logs/${n}-describe-stack-resources.json
}

function get_children(){
  for r in $(aws cloudformation describe-stack-resources --stack-name $1 --region $REGION --query 'StackResources[?ResourceType==`AWS::CloudFormation::Stack`].PhysicalResourceId' --output text); do
    gather_stack $r
    sleep 1
    get_children $r
  done
}

STACK_ARN=$(aws cloudformation describe-stacks --stack-name $STACK_ID --region $REGION --query 'Stacks[0].StackId' --output text)
gather_stack $STACK_ARN
get_children $STACK_ARN

echo ""
echo "gathering logs for lambda functions..."
for prefix in $FUNCTIONS; do
  for group in $(aws logs describe-log-groups --log-group-name-prefix /aws/lambda/${prefix} --query logGroups[].logGroupName --output text --region $REGION); do
    n=$(echo $group | awk -F/ '{print $4}')
    echo "getting logs from $group..."
    aws logs filter-log-events --log-group-name $group --query 'events[].{t: join(`: `, [to_string(timestamp), message])}' --output text --region $REGION > /tmp/eks-qs-logs/${n}.log
  done
done

echo ""
echo "gathering logs for cfn resource types..."
for group in $TYPE_LOG_GROUPS; do
  n=$(echo $group | awk -F/ '{print $4}')
  echo "getting logs from $group..."
  aws logs filter-log-events --log-group-name $group --query 'events[].{t: join(`: `, [to_string(timestamp), message])}' --output text --region $REGION > /tmp/eks-qs-logs/${n}.log
done

echo ""
echo "compressing logs..."
pwd=$(pwd)
cd /tmp/eks-qs-logs || exit 1
zip $pwd/logs.zip ./*
cd $pwd || exit 1
rm -rf /tmp/eks-qs-logs
echo ""
echo "logs saved to ${pwd}/logs.zip"