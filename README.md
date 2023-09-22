Example of `stream-loader` lib loading data to HDFS
Using `DeduplicatingRecordBatchingSink` which stores keys of seen messages and ensures only one message with given key will be written to storage

Commands:
    
    cd env
    docker compose up

    docker exec -it {container id} bash 
    hdfs dfs -ls /

Kafdrop
    
    http://localhost:19000/
    

Flags:
 
    -XX:MaxRamPercentage=80

Links:
 - https://github.com/big-data-europe/docker-hadoop
 - https://scalapb.github.io/docs/getting-started
 - https://github.com/lightbend/config