Bigtable Benchmark

This module contains benchmarking test scenarios, which would be idle to check the difference between **bigtable-core** client & **GCJ veneer** client.
after running for one method.

It accepts following parameters:

```bash
$ mvn clean package 
$ java -jar target/benchmarks.jar \
        -p projectId=[Project ID] \
        -p instanceId=[Instance ID] \
        -p benchmarkConfig="cellsPerRow/[#]/cellSize/[#]", ... \
        -p useBatch=false \
        -p useGCJ=true,false 
```


### Observations

    # Run complete. Total time: 00:46:30
    Benchmark                              (benchmarkConfig)   (instanceId)      (projectId)  (useBatch)  (useGcj)  Mode  Cnt      Score     Error  Units
    BigtableBenchmark.bulkScans  cellsPerRow/10/cellSize/100  [Instance ID]     [Project ID]       false      true  avgt   40  22994.955 ± 853.821  ms/op
    BigtableBenchmark.bulkScans  cellsPerRow/10/cellSize/100  [Instance ID]     [Project ID]       false     false  avgt   40  20798.961 ± 815.269  ms/op
    BigtableBenchmark.pointRead  cellsPerRow/10/cellSize/100  [Instance ID]     [Project ID]       false      true  avgt   40      4.536 ±   0.174  ms/op
    BigtableBenchmark.pointRead  cellsPerRow/10/cellSize/100  [Instance ID]     [Project ID]       false     false  avgt   40      4.172 ±   0.126  ms/op
    // Write Operation
    # Run complete. Total time: 00:34:15
    BigtableBenchmark.bulkWrite  cellsPerRow/10/cellSize/100  [Instance ID]     [Project ID]       false      true  avgt    5  49896.975 ± 6120.159  ms/op
    BigtableBenchmark.bulkWrite  cellsPerRow/10/cellSize/100  [Instance ID]     [Project ID]       false     false  avgt    5  55244.901 ± 4614.420  ms/op
    BigtableBenchmark.pointWrite cellsPerRow/10/cellSize/100  [Instance ID]     [Project ID]       false      true  avgt    5      5.013 ±    0.460  ms/op
    BigtableBenchmark.pointWrite cellsPerRow/10/cellSize/100  [Instance ID]     [Project ID]       false     false  avgt    5      5.189 ±    1.202  ms/op
     
    # Run complete. Total time: 00:18:53
    Benchmark                              (benchmarkConfig)   (instanceId)      (projectId)  (useBatch)  (useGcj)  Mode  Cnt     Score      Error   Units
    BigtableBenchmark.bulkScans  cellsPerRow/1/cellSize/1024  [Instance ID]     [Project ID]       false      true  avgt   40    8198.757 ±  586.135  ms/op
    BigtableBenchmark.bulkScans  cellsPerRow/1/cellSize/1024  [Instance ID]     [Project ID]       false     false  avgt   40    7641.325 ±  226.324  ms/op
    BigtableBenchmark.pointRead  cellsPerRow/1/cellSize/1024  [Instance ID]     [Project ID]       false      true  avgt   40       4.876 ±    0.420  ms/op
    BigtableBenchmark.pointRead  cellsPerRow/1/cellSize/1024  [Instance ID]     [Project ID]       false     false  avgt   40       4.232 ±    0.082  ms/op    
    // Write Operation
    BigtableBenchmark.bulkWrite  cellsPerRow/1/cellSize/1024  [Instance ID]     [Project ID]       false      true  avgt    5   19581.516 ±  4173.864 ms/op
    BigtableBenchmark.bulkWrite  cellsPerRow/1/cellSize/1024  [Instance ID]     [Project ID]       false     false  avgt    5   21315.619 ±  5975.093 ms/op
    BigtableBenchmark.pointWrite cellsPerRow/1/cellSize/1024  [Instance ID]     [Project ID]       false      true  avgt    5       5.207 ±     0.500 ms/op
    BigtableBenchmark.pointWrite cellsPerRow/1/cellSize/1024  [Instance ID]     [Project ID]       false     false  avgt    5       4.757 ±     0.603 ms/op

    # Run complete. Total time: 00:36:38
    Benchmark                             (benchmarkConfig)   (instanceId)      (projectId)  (useBatch)  (useGcj)  Mode  Cnt       Score       Error  Units
    BigtableBenchmark.bulkScans  cellsPerRow/100/cellSize/1  [Instance ID]     [Project ID]      false     true   avgt   <! --- DEADLINE_EXCEED Exception  -->
    BigtableBenchmark.bulkScans  cellsPerRow/100/cellSize/1  [Instance ID]     [Project ID]      false     false  avgt   10  107989.796 ± 32414.357  ms/op
    BigtableBenchmark.pointRead  cellsPerRow/100/cellSize/1  [Instance ID]     [Project ID]      false     true   avgt   10       5.254 ±     1.497  ms/op
    BigtableBenchmark.pointRead  cellsPerRow/100/cellSize/1  [Instance ID]     [Project ID]      false     false  avgt   10       5.529 ±     0.686  ms/op
    // Write Operation
    BigtableBenchmark.bulkWrite  cellsPerRow/100/cellSize/1  [Instance ID]     [Project ID]      false     true   avgt    5  274212.097 ± 39328.478  ms/op
    BigtableBenchmark.bulkWrite  cellsPerRow/100/cellSize/1  [Instance ID]     [Project ID]      false     false  avgt    5  280528.018 ± 63971.361  ms/op
