#!/bin/bash
 
set -e
 
# The easiest way to use this is to run it on the master node
# and then scp /tmp/EndToEndIT-snapshot.zip to the git repo.hbase shell <<EOF
 
# Create a compressed and uncompressed table and then snapshot them (Both tables must have same data)
hbase shell <<EOF
create 'test-table', {NAME => 'cf', VERSIONS => 4, BLOCKCACHE => true},  {SPLITS => ['10','20','30','40', '50', '60', '70', '80', '90']}
put 'test-table','00', 'cf:a', 'value0',1617828990
put 'test-table','01', 'cf:a', 'value1',1617828990
put 'test-table','02', 'cf:a', 'value2',1617828990
put 'test-table','03', 'cf:a', 'value3',1617828990
put 'test-table','04', 'cf:a', 'value4',1617828990
put 'test-table','05', 'cf:a', 'value5',1617828990
put 'test-table','06', 'cf:a', 'value6',1617828990
put 'test-table','07', 'cf:a', 'value7',1617828990
put 'test-table','08', 'cf:a', 'value8',1617828990
put 'test-table','09', 'cf:a', 'value9',1617828990
put 'test-table','10', 'cf:a', 'value10',1617828990
put 'test-table','11', 'cf:a', 'value11',1617828990
put 'test-table','12', 'cf:a', 'value12',1617828990
put 'test-table','13', 'cf:a', 'value13',1617828990
put 'test-table','14', 'cf:a', 'value14',1617828990
put 'test-table','15', 'cf:a', 'value15',1617828990
put 'test-table','16', 'cf:a', 'value16',1617828990
put 'test-table','17', 'cf:a', 'value17',1617828990
put 'test-table','18', 'cf:a', 'value18',1617828990
put 'test-table','19', 'cf:a', 'value19',1617828990
put 'test-table','20', 'cf:a', 'value20',1617828990
put 'test-table','21', 'cf:a', 'value21',1617828990
put 'test-table','22', 'cf:a', 'value22',1617828990
put 'test-table','23', 'cf:a', 'value23',1617828990
put 'test-table','24', 'cf:a', 'value24',1617828990
put 'test-table','25', 'cf:a', 'value25',1617828990
put 'test-table','26', 'cf:a', 'value26',1617828990
put 'test-table','27', 'cf:a', 'value27',1617828990
put 'test-table','28', 'cf:a', 'value28',1617828990
put 'test-table','29', 'cf:a', 'value29',1617828990
put 'test-table','30', 'cf:a', 'value30',1617828990
put 'test-table','31', 'cf:a', 'value31',1617828990
put 'test-table','32', 'cf:a', 'value32',1617828990
put 'test-table','33', 'cf:a', 'value33',1617828990
put 'test-table','34', 'cf:a', 'value34',1617828990
put 'test-table','35', 'cf:a', 'value35',1617828990
put 'test-table','36', 'cf:a', 'value36',1617828990
put 'test-table','37', 'cf:a', 'value37',1617828990
put 'test-table','38', 'cf:a', 'value38',1617828990
put 'test-table','39', 'cf:a', 'value39',1617828990
put 'test-table','40', 'cf:a', 'value40',1617828990
put 'test-table','41', 'cf:a', 'value41',1617828990
put 'test-table','42', 'cf:a', 'value42',1617828990
put 'test-table','43', 'cf:a', 'value43',1617828990
put 'test-table','44', 'cf:a', 'value44',1617828990
put 'test-table','45', 'cf:a', 'value45',1617828990
put 'test-table','46', 'cf:a', 'value46',1617828990
put 'test-table','47', 'cf:a', 'value47',1617828990
put 'test-table','48', 'cf:a', 'value48',1617828990
put 'test-table','49', 'cf:a', 'value49',1617828990
put 'test-table','50', 'cf:a', 'value50',1617828990
put 'test-table','51', 'cf:a', 'value51',1617828990
put 'test-table','52', 'cf:a', 'value52',1617828990
put 'test-table','53', 'cf:a', 'value53',1617828990
put 'test-table','54', 'cf:a', 'value54',1617828990
put 'test-table','55', 'cf:a', 'value55',1617828990
put 'test-table','56', 'cf:a', 'value56',1617828990
put 'test-table','57', 'cf:a', 'value57',1617828990
put 'test-table','58', 'cf:a', 'value58',1617828990
put 'test-table','59', 'cf:a', 'value59',1617828990
put 'test-table','60', 'cf:a', 'value60',1617828990
put 'test-table','61', 'cf:a', 'value61',1617828990
put 'test-table','62', 'cf:a', 'value62',1617828990
put 'test-table','63', 'cf:a', 'value63',1617828990
put 'test-table','64', 'cf:a', 'value64',1617828990
put 'test-table','65', 'cf:a', 'value65',1617828990
put 'test-table','66', 'cf:a', 'value66',1617828990
put 'test-table','67', 'cf:a', 'value67',1617828990
put 'test-table','68', 'cf:a', 'value68',1617828990
put 'test-table','69', 'cf:a', 'value69',1617828990
put 'test-table','70', 'cf:a', 'value70',1617828990
put 'test-table','71', 'cf:a', 'value71',1617828990
put 'test-table','72', 'cf:a', 'value72',1617828990
put 'test-table','73', 'cf:a', 'value73',1617828990
put 'test-table','74', 'cf:a', 'value74',1617828990
put 'test-table','75', 'cf:a', 'value75',1617828990
put 'test-table','76', 'cf:a', 'value76',1617828990
put 'test-table','77', 'cf:a', 'value77',1617828990
put 'test-table','78', 'cf:a', 'value78',1617828990
put 'test-table','79', 'cf:a', 'value79',1617828990
put 'test-table','80', 'cf:a', 'value80',1617828990
put 'test-table','81', 'cf:a', 'value81',1617828990
put 'test-table','82', 'cf:a', 'value82',1617828990
put 'test-table','83', 'cf:a', 'value83',1617828990
put 'test-table','84', 'cf:a', 'value84',1617828990
put 'test-table','85', 'cf:a', 'value85',1617828990
put 'test-table','86', 'cf:a', 'value86',1617828990
put 'test-table','87', 'cf:a', 'value87',1617828990
put 'test-table','88', 'cf:a', 'value88',1617828990
put 'test-table','89', 'cf:a', 'value89',1617828990
put 'test-table','90', 'cf:a', 'value90',1617828990
put 'test-table','91', 'cf:a', 'value91',1617828990
put 'test-table','92', 'cf:a', 'value92',1617828990
put 'test-table','93', 'cf:a', 'value93',1617828990
put 'test-table','94', 'cf:a', 'value94',1617828990
put 'test-table','95', 'cf:a', 'value95',1617828990
put 'test-table','96', 'cf:a', 'value96',1617828990
put 'test-table','97', 'cf:a', 'value97',1617828990
put 'test-table','98', 'cf:a', 'value98',1617828990
put 'test-table','99', 'cf:a', 'value99',1617828990
snapshot 'test-table', 'test-snapshot'
 
create 'test-snappy-table', {NAME => 'cf', VERSIONS => 4, BLOCKCACHE => true, COMPRESSION => 'SNAPPY'},  {SPLITS => ['10','20','30','40', '50', '60', '70', '80', '90']}
put 'test-snappy-table','00', 'cf:a', 'value0',1617828990
put 'test-snappy-table','01', 'cf:a', 'value1',1617828990
put 'test-snappy-table','02', 'cf:a', 'value2',1617828990
put 'test-snappy-table','03', 'cf:a', 'value3',1617828990
put 'test-snappy-table','04', 'cf:a', 'value4',1617828990
put 'test-snappy-table','05', 'cf:a', 'value5',1617828990
put 'test-snappy-table','06', 'cf:a', 'value6',1617828990
put 'test-snappy-table','07', 'cf:a', 'value7',1617828990
put 'test-snappy-table','08', 'cf:a', 'value8',1617828990
put 'test-snappy-table','09', 'cf:a', 'value9',1617828990
put 'test-snappy-table','10', 'cf:a', 'value10',1617828990
put 'test-snappy-table','11', 'cf:a', 'value11',1617828990
put 'test-snappy-table','12', 'cf:a', 'value12',1617828990
put 'test-snappy-table','13', 'cf:a', 'value13',1617828990
put 'test-snappy-table','14', 'cf:a', 'value14',1617828990
put 'test-snappy-table','15', 'cf:a', 'value15',1617828990
put 'test-snappy-table','16', 'cf:a', 'value16',1617828990
put 'test-snappy-table','17', 'cf:a', 'value17',1617828990
put 'test-snappy-table','18', 'cf:a', 'value18',1617828990
put 'test-snappy-table','19', 'cf:a', 'value19',1617828990
put 'test-snappy-table','20', 'cf:a', 'value20',1617828990
put 'test-snappy-table','21', 'cf:a', 'value21',1617828990
put 'test-snappy-table','22', 'cf:a', 'value22',1617828990
put 'test-snappy-table','23', 'cf:a', 'value23',1617828990
put 'test-snappy-table','24', 'cf:a', 'value24',1617828990
put 'test-snappy-table','25', 'cf:a', 'value25',1617828990
put 'test-snappy-table','26', 'cf:a', 'value26',1617828990
put 'test-snappy-table','27', 'cf:a', 'value27',1617828990
put 'test-snappy-table','28', 'cf:a', 'value28',1617828990
put 'test-snappy-table','29', 'cf:a', 'value29',1617828990
put 'test-snappy-table','30', 'cf:a', 'value30',1617828990
put 'test-snappy-table','31', 'cf:a', 'value31',1617828990
put 'test-snappy-table','32', 'cf:a', 'value32',1617828990
put 'test-snappy-table','33', 'cf:a', 'value33',1617828990
put 'test-snappy-table','34', 'cf:a', 'value34',1617828990
put 'test-snappy-table','35', 'cf:a', 'value35',1617828990
put 'test-snappy-table','36', 'cf:a', 'value36',1617828990
put 'test-snappy-table','37', 'cf:a', 'value37',1617828990
put 'test-snappy-table','38', 'cf:a', 'value38',1617828990
put 'test-snappy-table','39', 'cf:a', 'value39',1617828990
put 'test-snappy-table','40', 'cf:a', 'value40',1617828990
put 'test-snappy-table','41', 'cf:a', 'value41',1617828990
put 'test-snappy-table','42', 'cf:a', 'value42',1617828990
put 'test-snappy-table','43', 'cf:a', 'value43',1617828990
put 'test-snappy-table','44', 'cf:a', 'value44',1617828990
put 'test-snappy-table','45', 'cf:a', 'value45',1617828990
put 'test-snappy-table','46', 'cf:a', 'value46',1617828990
put 'test-snappy-table','47', 'cf:a', 'value47',1617828990
put 'test-snappy-table','48', 'cf:a', 'value48',1617828990
put 'test-snappy-table','49', 'cf:a', 'value49',1617828990
put 'test-snappy-table','50', 'cf:a', 'value50',1617828990
put 'test-snappy-table','51', 'cf:a', 'value51',1617828990
put 'test-snappy-table','52', 'cf:a', 'value52',1617828990
put 'test-snappy-table','53', 'cf:a', 'value53',1617828990
put 'test-snappy-table','54', 'cf:a', 'value54',1617828990
put 'test-snappy-table','55', 'cf:a', 'value55',1617828990
put 'test-snappy-table','56', 'cf:a', 'value56',1617828990
put 'test-snappy-table','57', 'cf:a', 'value57',1617828990
put 'test-snappy-table','58', 'cf:a', 'value58',1617828990
put 'test-snappy-table','59', 'cf:a', 'value59',1617828990
put 'test-snappy-table','60', 'cf:a', 'value60',1617828990
put 'test-snappy-table','61', 'cf:a', 'value61',1617828990
put 'test-snappy-table','62', 'cf:a', 'value62',1617828990
put 'test-snappy-table','63', 'cf:a', 'value63',1617828990
put 'test-snappy-table','64', 'cf:a', 'value64',1617828990
put 'test-snappy-table','65', 'cf:a', 'value65',1617828990
put 'test-snappy-table','66', 'cf:a', 'value66',1617828990
put 'test-snappy-table','67', 'cf:a', 'value67',1617828990
put 'test-snappy-table','68', 'cf:a', 'value68',1617828990
put 'test-snappy-table','69', 'cf:a', 'value69',1617828990
put 'test-snappy-table','70', 'cf:a', 'value70',1617828990
put 'test-snappy-table','71', 'cf:a', 'value71',1617828990
put 'test-snappy-table','72', 'cf:a', 'value72',1617828990
put 'test-snappy-table','73', 'cf:a', 'value73',1617828990
put 'test-snappy-table','74', 'cf:a', 'value74',1617828990
put 'test-snappy-table','75', 'cf:a', 'value75',1617828990
put 'test-snappy-table','76', 'cf:a', 'value76',1617828990
put 'test-snappy-table','77', 'cf:a', 'value77',1617828990
put 'test-snappy-table','78', 'cf:a', 'value78',1617828990
put 'test-snappy-table','79', 'cf:a', 'value79',1617828990
put 'test-snappy-table','80', 'cf:a', 'value80',1617828990
put 'test-snappy-table','81', 'cf:a', 'value81',1617828990
put 'test-snappy-table','82', 'cf:a', 'value82',1617828990
put 'test-snappy-table','83', 'cf:a', 'value83',1617828990
put 'test-snappy-table','84', 'cf:a', 'value84',1617828990
put 'test-snappy-table','85', 'cf:a', 'value85',1617828990
put 'test-snappy-table','86', 'cf:a', 'value86',1617828990
put 'test-snappy-table','87', 'cf:a', 'value87',1617828990
put 'test-snappy-table','88', 'cf:a', 'value88',1617828990
put 'test-snappy-table','89', 'cf:a', 'value89',1617828990
put 'test-snappy-table','90', 'cf:a', 'value90',1617828990
put 'test-snappy-table','91', 'cf:a', 'value91',1617828990
put 'test-snappy-table','92', 'cf:a', 'value92',1617828990
put 'test-snappy-table','93', 'cf:a', 'value93',1617828990
put 'test-snappy-table','94', 'cf:a', 'value94',1617828990
put 'test-snappy-table','95', 'cf:a', 'value95',1617828990
put 'test-snappy-table','96', 'cf:a', 'value96',1617828990
put 'test-snappy-table','97', 'cf:a', 'value97',1617828990
put 'test-snappy-table','98', 'cf:a', 'value98',1617828990
put 'test-snappy-table','99', 'cf:a', 'value99',1617828990
 
snapshot 'test-snappy-table', 'test-snappy-snapshot'
 
list_snapshots
EOF
 
mkdir /tmp/EndToEndIT-snapshot
# Export the snapshot
hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot -snapshot test-snapshot -copy-to /tmp/EndToEndIT-snapshot/data -mappers 16
hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot -snapshot test-snappy-snapshot -copy-to /tmp/EndToEndIT-snapshot/data -mappers 16
 
# Copy the data to local filesystem
hadoop fs -copyToLocal /tmp/EndToEndIT-snapshot/ /tmp/
 
# You only need 1 hashtable output as logically the data is same in compressed and uncompressed tables.
hbase org.apache.hadoop.hbase.mapreduce.HashTable --batchsize=10 --numhashfiles=10 test-table /tmp/EndToEndIT-snapshot/hashtable
 
# Export the snapshot
cd /tmp/EndToEndIT-snapshot
zip -r /tmp/EndToEndIT-snapshot.zip data hashtable
 
# Cleanup the HBase resources
hbase shell <<EOF
disable 'test-table'
drop 'test-table'
 
disable 'test-snappy-table'
drop 'test-snappy-table'
 
delete_snapshot 'test-snapshot'
delete_snapshot 'test-snappy-snapshot'
EOF