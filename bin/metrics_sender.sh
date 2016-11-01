#!/usr/bin/env bash
url=http://$1/ws/v1/timeline/metrics
while [ 1 ]
do
json="{\"metrics\":[{\"metricName\":\"cpu_user\",\"appId\":\"1\",\"hostName\":\"c6402.zui\",\"timestamp\":0,\"startTime\":1407949812,
\"metricValues\":{\"1407949812\":1.0,\"1407949912\":1.8,\"1407950002\":0.7}},{\"metricName\":\"mem_free\",\"appId\":\"2\",
\"instanceId\":\"3\",\"hostName\":\"c6401.zth\",\"timestamp\":0,\"startTime\":1407949812,\"metricValues\":{\"1407949812\":2.5,
\"1407949912\":3.0,\"1407950002\":0.9}}]}"

curl -i -X POST -H "Content-Type: application/json" -d "${json}" ${url}
sleep 5
done
