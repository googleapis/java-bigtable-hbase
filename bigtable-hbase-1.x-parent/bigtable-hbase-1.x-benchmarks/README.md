Bigtable Benchmark

This module contains benchmarking test scenarios, which would be idle to check the difference between **bigtable-core** client & **GCJ veneer** client.
after running for one method.

It accepts following parameters:

```bash
$ mvn clean package 
$ java -jar target/benchmarks.jar \
        -p projectId=[Project ID] \
        -p instanceId=[Instance ID] \
        -p rowShape="cellsPerRow/[#]/cellSize/[#]", ... \
        -p useBatch=false \
        -p useGCJ=true,false 
```
