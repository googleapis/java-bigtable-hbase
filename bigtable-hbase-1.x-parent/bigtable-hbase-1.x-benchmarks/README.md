Bigtable Benchmark

This module contains benchmarking test scenarios, which would be idle to check the difference between **bigtable-core** client & **GCJ veneer** client.
after running for one method.

It accepts these params:

```bash
$ mvn clean install 
$ java -jar target/benchmarks.jar \
        -p projectId=[Project ID] \
        -p instanceId=[Instance ID] \
        -p numRows=100 \
        -p cellsPerRow=10 \
        -p cellSize=100 \
        -p numKeys=20 \
        -p bulkReadCellsLen=1000 \
        -p useBatch=false \
        -p useGCJ=true,false 
```


### Observations
