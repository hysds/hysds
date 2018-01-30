#!/bin/bash

QUEUE=$1
ASG=$2
aws autoscaling put-scaling-policy --auto-scaling-group-name $ASG --policy-name "${QUEUE}-${ASG}-target-tracking" --policy-type TargetTrackingScaling --target-tracking-configuration "{
            \"CustomizedMetricSpecification\": {
              \"MetricName\": \"JobsWaitingPerInstance-${QUEUE}-${ASG}\",
              \"Namespace\": \"HySDS\",
              \"Dimensions\": [
                {
                  \"Name\": \"AutoScalingGroupName\",
                  \"Value\": \"${ASG}\"
                },
                {
                  \"Name\": \"Queue\",
                  \"Value\": \"${QUEUE}\"
                }
              ],
              \"Statistic\": \"Maximum\"
            },
            \"TargetValue\": 1.0,
            \"DisableScaleIn\": true
          }"
