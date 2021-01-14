# Generating the test HBase snapshot for HBase snapshot import integration tests

The file `generate_test_data.txt` is an HBase command line command sequence 
used to generated the testing HBase snapshot data.

If you need to modify the test data used by `bigtable-dataflow-parent/bigtable-beam-import/src/test/java/com/google/cloud/bigtable/beam/hbasesnapshots/EndToEndIT.java`, 
Please make sure you have HBase installed and export `<path-to-hbase>/bin` to your PATH. 

Then:

    $ hbase shell ./generate_test_data.txt
    $ hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot -Dmapreduce.framework.name=local -snapshot test-snapshot -copy-to file:///<local-path>/data
    
    $ cd <local-path>
    $ gsutil -m cp -r ./data/ gs://<test-bucket>/integration-test/
    
After this, you use be able to run the integration test with your new data by specifying
`-Dcloud.test.data.folder=gs://<test-bucket>/integration-test/`