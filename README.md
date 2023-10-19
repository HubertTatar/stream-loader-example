Examples of `stream-loader` lib loading data to HDFS

Loading `proto` data from Kafka to HDFS.

    SimpleConfiguration.buildSink         - sink loading every message from Kafka
    SimpleConfiguration.deduplicatingSink - sink loading unique messages from Kafka
    SimpleRunner                          - simple single threaded loader
    SimpleRunner                          - simple single threaded loader
    LoaderRunner                          - loader allowing to set concurrency level
    DataProducerRunner                    - generate proto to Kafka  

Commands:
    
    cd env
    docker compose up

    docker ps | grep namenode | awk '{print $1}' - get id
    docker exec -it {container id} bash 
    hdfs dfs -ls /

Tools
    
    kafdrop - http://localhost:19000/
    zoonavigator - http://localhost:19001/
    prmoetheus - http://localhost:19002/
    graphana - http://localhost:19003/
    spark master - http://localhost:8080/
    spark worker 1 - http://localhost:8081/
    spark worker 2 - http://localhost:8082/
    spark history - http://localhost:18081/
    spark app - use https://github.com/HubertTatar/sle-spark
Grafana

    Register new source as `http://prometheus:9090`
    Import dashboard from `dashboard.json`
    
Metrics:
    
    http://localhost:7071/metrics

Flags:
 
    -XX:MaxRamPercentage=80

Links:
 - https://github.com/big-data-europe/docker-hadoop
 - https://scalapb.github.io/docs/getting-started
 - https://github.com/lightbend/config
 - https://prometheus.io/docs/prometheus/latest/configuration/configuration/