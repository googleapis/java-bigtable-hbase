Bigtable Benchmark

This module contains benchmarking test scenarios, which uses **GCJ veneer** client.

It accepts following parameters:

```bash
$ mvn clean package 
$ java -jar target/benchmarks.jar \
        -p projectId=[Project ID] \
        -p instanceId=[Instance ID] \
        -p rowShape="cellsPerRow/[#]/cellSize/[#]", ... \
        -p useBatch=false
```
