#!/bin/bash

aws application-autoscaling register-scalable-target --service-namespace ec2 \
  --resource-id spot-fleet-request/sfr-df97e1f4-cecd-47ca-84df-db682872bdc7 \
  --scalable-dimension ec2:spot-fleet-request:TargetCapacity \
  --min-capacity 0 --max-capacity 6 --role-arn arn:aws:iam::136244619446:role/aws-ec2-spot-fleet-autoscale-role
aws application-autoscaling put-scaling-policy --policy-name test-target-tracking-spot_fleet \
  --service-namespace ec2 --resource-id spot-fleet-request/sfr-df97e1f4-cecd-47ca-84df-db682872bdc7 \
  --scalable-dimension ec2:spot-fleet-request:TargetCapacity --policy-type TargetTrackingScaling \
  --target-tracking-scaling-policy-configuration '{
  "TargetValue": 1.0,
  "CustomizedMetricSpecification": {
    "MetricName": "JobsWaitingPerInstance",
    "Namespace": "HySDS",
    "Dimensions": [
      {
        "Name": "AutoScalingGroupName",
        "Value": "grfn-ops-amzn-asg"
      },
      {
        "Name": "Queue",
        "Value": "grfn-job_worker-large"
      }
    ],
    "Statistic": "Maximum",
    "Unit": "Count"
  },
  "ScaleOutCooldown": 60,
  "ScaleInCooldown": 60
}'
