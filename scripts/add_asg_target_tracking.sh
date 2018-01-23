#!/bin/bash

aws autoscaling put-scaling-policy --auto-scaling-group-name grfn-ops-amzn-asg --policy-name test-target-tracking --policy-type TargetTrackingScaling --target-tracking-configuration '{
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
            "TargetValue": 1.0,
            "DisableScaleIn": true
          }'
