Example of `stream-loader` lib loading data to HDFS
Using `DeduplicatingRecordBatchingSink` which stores keys of seen messages and ensures only one message with given key will be written to storage

Commands:
    
    cd env
    docker compose up

    docker exec -it {container id} bash 
    hdfs dfs -ls /

Tools
    
    kafdrop - http://localhost:19000/
    zoonavigator - http://localhost:19001/
    prmoetheus - http://localhost:19002/
    graphana - http://localhost:19003/

Grafana

    Register new source as `http://prometheus:9090`
    Import dashboard from `dashboard.json`
    
Metrics:
    
    http://localhost:8081/metrics

Flags:
 
    -XX:MaxRamPercentage=80

Links:
 - https://github.com/big-data-europe/docker-hadoop
 - https://scalapb.github.io/docs/getting-started
 - https://github.com/lightbend/config
 - https://prometheus.io/docs/prometheus/latest/configuration/configuration/