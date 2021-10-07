# Changelog

## [2.0.0-beta1](https://www.github.com/googleapis/java-bigtable-hbase/compare/v2.0.0-alpha2...v2.0.0-beta1) (2021-10-07)


### Bug Fixes

* adding validation for snapshot name for hbase import pipeline ([#3203](https://www.github.com/googleapis/java-bigtable-hbase/issues/3203)) ([fa9991a](https://www.github.com/googleapis/java-bigtable-hbase/commit/fa9991a2703c0faf4a1ba5737f5844619a497c17))
* Clean up RowResultAdapter ([#3267](https://www.github.com/googleapis/java-bigtable-hbase/issues/3267)) ([1ccf063](https://www.github.com/googleapis/java-bigtable-hbase/commit/1ccf0634c73c5ccef1efc612009ed66e11798021))


### Dependencies

* update bigtable.version to v2.1.4 ([#3246](https://www.github.com/googleapis/java-bigtable-hbase/issues/3246)) ([8636efb](https://www.github.com/googleapis/java-bigtable-hbase/commit/8636efb8ba321d911f812a20e347f89a43365ec8))
* update dependency com.google.cloud:google-cloud-bigtable-emulator to v0.138.4 ([#3247](https://www.github.com/googleapis/java-bigtable-hbase/issues/3247)) ([7a3057f](https://www.github.com/googleapis/java-bigtable-hbase/commit/7a3057fbdec07758d8c24d544b6ae371f4afa288))
* update dependency org.codehaus.plexus:plexus-utils to v3.4.1 ([#3249](https://www.github.com/googleapis/java-bigtable-hbase/issues/3249)) ([dfb729f](https://www.github.com/googleapis/java-bigtable-hbase/commit/dfb729f5a4ab71f8789f304942e9154d6f546273))
* update jmh.version to v1.33 ([#3254](https://www.github.com/googleapis/java-bigtable-hbase/issues/3254)) ([ce8110e](https://www.github.com/googleapis/java-bigtable-hbase/commit/ce8110e7639c7524f962282a6d397a33253bca62))


### Miscellaneous Chores

* bump next tag to 2.0.0-beta1 ([#3277](https://www.github.com/googleapis/java-bigtable-hbase/issues/3277)) ([499d48b](https://www.github.com/googleapis/java-bigtable-hbase/commit/499d48bbad69c1639cfc523bfb4d9226dd4c4a65))
* make next tag 2.0.0-alpha3 ([#3207](https://www.github.com/googleapis/java-bigtable-hbase/issues/3207)) ([a6241e1](https://www.github.com/googleapis/java-bigtable-hbase/commit/a6241e1c800592e560d6bdd2bfa832e783bc1ed2))

## [2.0.0-alpha2](https://www.github.com/googleapis/java-bigtable-hbase/compare/v2.0.0-alpha1...v2.0.0-alpha2) (2021-08-19)


### ⚠ BREAKING CHANGES

* migrate to java 8 (#3189)

### Features

* Add support for renaming tables in schema translator ([#3154](https://www.github.com/googleapis/java-bigtable-hbase/issues/3154)) ([7d49bba](https://www.github.com/googleapis/java-bigtable-hbase/commit/7d49bba33c0b722e953ae20e4e393a7fd06a6567))
* migrate to java 8 ([#3189](https://www.github.com/googleapis/java-bigtable-hbase/issues/3189)) ([948b99f](https://www.github.com/googleapis/java-bigtable-hbase/commit/948b99ffe9dfc85d647f5d480544e91bd0cf6a0a))


### Miscellaneous Chores

* bump next tag to 2.0.0-alpha2 ([#3170](https://www.github.com/googleapis/java-bigtable-hbase/issues/3170)) ([25c4bfb](https://www.github.com/googleapis/java-bigtable-hbase/commit/25c4bfb49342d2743c822c850932acf5dcc8735c))

## [2.0.0-alpha1](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.23.0...v2.0.0-alpha1) (2021-07-19)

**Note: This alpha release is a work-in-progress. For the latest stable version of java-bigtable-hbase, please refer to version [1.23.0](https://github.com/googleapis/java-bigtable-hbase/releases/tag/v1.23.0).**

This is the first alpha release of Bigtable HBase 2.0.0. This release switches the core Bigtable layer to the [java-bigtable](https://www.github.com/googleapis/java-bigtable) library. 
This is primarily an implementation detail change. Currently, users can opt out of this via the `BIGTABLE_USE_GCJ_CLIENT` configuration option to use the existing bigtable-client-core layer. 
This option is primarily intended for testing during the alpha period. Example:
```
Configuration configuration = new Configuration(false);
...
configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT, false);
```

In future releases, the configuration option to toggle the use of client core on or off will be removed, and the library 
will no longer use bigtable-client-core to connect to the Bigtable API.

Other notable changes include:
* Deadlines are enabled by default (google.bigtable.rpc.use.timeouts)
* Retry settings have been tweaked (timeouts, exponential backoff)
* Error handling has been improved to always be a subclass of IOException and to include the caller’s stack trace for asynchronous calls
* Dependencies have been improved (mapreduce byo-hadoop, etc)

We look forward to hearing your feedback! Please let us know any comments or issues in our issue tracker.

Complete release notes below:

### Features

* Ability to import HBase Snapshot data into Cloud Bigtable using Dataflow ([#2755](https://www.github.com/googleapis/java-bigtable-hbase/issues/2755)) ([5b3ab2b](https://www.github.com/googleapis/java-bigtable-hbase/commit/5b3ab2b9b4a751d1b16bba04e1e08e2604d73faa))
* add `gcf-owl-bot[bot]` to `ignoreAuthors` ([#2974](https://www.github.com/googleapis/java-bigtable-hbase/issues/2974)) ([96d43d2](https://www.github.com/googleapis/java-bigtable-hbase/commit/96d43d28ef63508bd51d54713487e525d6591e2f))
* Add a new pipeline to validate data imported from HBase ([#2828](https://www.github.com/googleapis/java-bigtable-hbase/issues/2828)) ([fa07a90](https://www.github.com/googleapis/java-bigtable-hbase/commit/fa07a9045ed15f76fce78d3da570504c229292e2))
* add client-core metrics to veneer ([#2978](https://www.github.com/googleapis/java-bigtable-hbase/issues/2978)) ([68c2773](https://www.github.com/googleapis/java-bigtable-hbase/commit/68c27737b02cff246b772c7e88dee304a6ca05ca))
* Add keepalive  in grpc channelbuilder ([#2682](https://www.github.com/googleapis/java-bigtable-hbase/issues/2682)) ([2a732d2](https://www.github.com/googleapis/java-bigtable-hbase/commit/2a732d213a6dc35d4dc5bb003009ded8dab3707e))
* add option to configure tracing cookie ([#3021](https://www.github.com/googleapis/java-bigtable-hbase/issues/3021)) ([228a60e](https://www.github.com/googleapis/java-bigtable-hbase/commit/228a60e7ab92c8d44f28495be00a7c0807ff6449))
* adding client wrapper interfaces ([#2406](https://www.github.com/googleapis/java-bigtable-hbase/issues/2406)) ([1ad48d9](https://www.github.com/googleapis/java-bigtable-hbase/commit/1ad48d9418e4d25180ca3dc66aaefcf80e00314b))
* adding custom User agent for dataflow jobs. ([#2952](https://www.github.com/googleapis/java-bigtable-hbase/issues/2952)) ([d9843b8](https://www.github.com/googleapis/java-bigtable-hbase/commit/d9843b8a247473570e6f357b9c4b13a2e2559003))
* enable directpath ([#2271](https://www.github.com/googleapis/java-bigtable-hbase/issues/2271)) ([d384208](https://www.github.com/googleapis/java-bigtable-hbase/commit/d384208c1d727fe99e57a10cc7ba8a5e4b75a041))
* extend RowCell to include labels field ([#2397](https://www.github.com/googleapis/java-bigtable-hbase/issues/2397)) ([80a85fe](https://www.github.com/googleapis/java-bigtable-hbase/commit/80a85fee02958d0fbe4fb32c6c809db7c0803358))
* implement veneer metrics part 1 - implement MetricsApiTracerAdapter ([#2631](https://www.github.com/googleapis/java-bigtable-hbase/issues/2631)) ([bf8a7e3](https://www.github.com/googleapis/java-bigtable-hbase/commit/bf8a7e3e2ae2b7eaa6ad4170f2f3ceb38ddfa48a))
* improve handling when crossing async and sync boundaries ([#2976](https://www.github.com/googleapis/java-bigtable-hbase/issues/2976)) ([0a63256](https://www.github.com/googleapis/java-bigtable-hbase/commit/0a632566c13c2e377586945a4680789e8bed047f))
* managed backups implementation ([#2643](https://www.github.com/googleapis/java-bigtable-hbase/issues/2643)) ([f7afd50](https://www.github.com/googleapis/java-bigtable-hbase/commit/f7afd506ae1f205f81d6726d7212a5e4556f37c0))
* mapreduce snapshot import to cloud bigtable ([#2851](https://www.github.com/googleapis/java-bigtable-hbase/issues/2851)) ([d8ad7dd](https://www.github.com/googleapis/java-bigtable-hbase/commit/d8ad7ddddc695f3af2a8d6f0c116858fd7883970))
* minimize bloat in mapreduce module ([#2895](https://www.github.com/googleapis/java-bigtable-hbase/issues/2895)) ([0cd7896](https://www.github.com/googleapis/java-bigtable-hbase/commit/0cd78961b832a381a7b0d5cef4d4db1d45ec63f0))
* Schema translator for HBase. ([#2901](https://www.github.com/googleapis/java-bigtable-hbase/issues/2901)) ([da9e036](https://www.github.com/googleapis/java-bigtable-hbase/commit/da9e0365048833af1d4f394c176a1a267d881c8a))
* tune default settings to align with java-bigtable ([#2439](https://www.github.com/googleapis/java-bigtable-hbase/issues/2439)) ([4aedcd9](https://www.github.com/googleapis/java-bigtable-hbase/commit/4aedcd9a572ef5ad246628654e06d31348dfd383))
* use veneer by default ([#2980](https://www.github.com/googleapis/java-bigtable-hbase/issues/2980)) ([aee2f4e](https://www.github.com/googleapis/java-bigtable-hbase/commit/aee2f4e60ca28a535ebcd41826752dc781e52921))


### Bug Fixes

* 1543 One line batch settings ([#1950](https://www.github.com/googleapis/java-bigtable-hbase/issues/1950)) ([2bc252a](https://www.github.com/googleapis/java-bigtable-hbase/commit/2bc252a2529b21183b2b43302a07981ec212847b))
* 1770 Fuzzy keys with integer values above 127 match no rows ([#1923](https://www.github.com/googleapis/java-bigtable-hbase/issues/1923)) ([9802eda](https://www.github.com/googleapis/java-bigtable-hbase/commit/9802eda3b6b7f18cd9df1e9ed797db36e3f4e94a))
* 2004 - NPE when calling Watchdog.stop() twice ([#2005](https://www.github.com/googleapis/java-bigtable-hbase/issues/2005)) ([b638828](https://www.github.com/googleapis/java-bigtable-hbase/commit/b638828f1f3a8865280727ac02f46c9f466c97f0))
* 2788 Using bom files to avoid possible version discrepancy among grpc ([#2789](https://www.github.com/googleapis/java-bigtable-hbase/issues/2789)) ([6e77eac](https://www.github.com/googleapis/java-bigtable-hbase/commit/6e77eacd0510eb527f4cbbec699ac1132f70289d))
* 691 SingleColumnValueFilter NOT_EQUALS empty value not working ([#1946](https://www.github.com/googleapis/java-bigtable-hbase/issues/1946)) ([c5d5c7e](https://www.github.com/googleapis/java-bigtable-hbase/commit/c5d5c7ea31c823a64cb00ee70617677682472565))
* add bigtable-hbase version to user agent ([#3033](https://www.github.com/googleapis/java-bigtable-hbase/issues/3033)) ([7c45f2c](https://www.github.com/googleapis/java-bigtable-hbase/commit/7c45f2c4089bf5c0f081804edd2818832fd93de4))
* add extraFiles to 1x branch ([#3160](https://www.github.com/googleapis/java-bigtable-hbase/issues/3160)) ([f6c7824](https://www.github.com/googleapis/java-bigtable-hbase/commit/f6c7824a257240f8618c717465fae19d0765b475))
* beam warning about accessing template values ([#2400](https://www.github.com/googleapis/java-bigtable-hbase/issues/2400)) ([443a40c](https://www.github.com/googleapis/java-bigtable-hbase/commit/443a40c65f40089a5cc41b5304a1422d1f8cd7ab)), closes [#2389](https://www.github.com/googleapis/java-bigtable-hbase/issues/2389)
* bigtable-hbase-2.x-hadoop incompatible with hbase-shaded-client 2.x ([#2773](https://www.github.com/googleapis/java-bigtable-hbase/issues/2773)) ([1792c60](https://www.github.com/googleapis/java-bigtable-hbase/commit/1792c6080ad4c2c59b32e830353e438d83a8d01f))
* bigtable-hbase-tools version ([#2920](https://www.github.com/googleapis/java-bigtable-hbase/issues/2920)) ([551b49f](https://www.github.com/googleapis/java-bigtable-hbase/commit/551b49f5c2f8316ac9619e9a850f8c8b6c87eb47))
* BigtableSession is never closed by Reader causing "ManagedChanne… ([#2782](https://www.github.com/googleapis/java-bigtable-hbase/issues/2782)) ([5340db5](https://www.github.com/googleapis/java-bigtable-hbase/commit/5340db59ca2e78c513574b04f2173dea50fb637d))
* **build:** update return codes for build scripts ([#2583](https://www.github.com/googleapis/java-bigtable-hbase/issues/2583)) ([96e8d8e](https://www.github.com/googleapis/java-bigtable-hbase/commit/96e8d8ec38fcb9f8b3b678bcbba91de402f32c36))
* close BigtableInstanceAdminClient and batcher ([#2963](https://www.github.com/googleapis/java-bigtable-hbase/issues/2963)) ([77d5b21](https://www.github.com/googleapis/java-bigtable-hbase/commit/77d5b21c0da0972442f89df649709a13fd22f04f))
* conversion from hbase to veneer settings ([#2912](https://www.github.com/googleapis/java-bigtable-hbase/issues/2912)) ([5c12fd8](https://www.github.com/googleapis/java-bigtable-hbase/commit/5c12fd8ab7458ea5775d89c04c5e115ac4117f50))
* deadlock scenario from BulkReadVeneerApi and fixed flaky tests ([#2484](https://www.github.com/googleapis/java-bigtable-hbase/issues/2484)) ([e4cd4ef](https://www.github.com/googleapis/java-bigtable-hbase/commit/e4cd4ef7b38777d1692a15d5f2182889271f6645))
* dont use channel pools for admin api for veneer ([#2917](https://www.github.com/googleapis/java-bigtable-hbase/issues/2917)) ([d9d54c3](https://www.github.com/googleapis/java-bigtable-hbase/commit/d9d54c320e635b088a5b8f0879e9f6c9f251aeba))
* enabling the integration test on the correct kokoro target ([#2911](https://www.github.com/googleapis/java-bigtable-hbase/issues/2911)) ([2c5492d](https://www.github.com/googleapis/java-bigtable-hbase/commit/2c5492da8f69e2861451046e237e71ef623dee0a))
* fix race condition in auto flush ([#2772](https://www.github.com/googleapis/java-bigtable-hbase/issues/2772)) ([ff8e489](https://www.github.com/googleapis/java-bigtable-hbase/commit/ff8e489cdca9aa208f71984f5cef46ff95884bd3))
* fix retry when rowCount == rowsLimit ([#2931](https://www.github.com/googleapis/java-bigtable-hbase/issues/2931)) ([c5da82f](https://www.github.com/googleapis/java-bigtable-hbase/commit/c5da82fe16196be6c62277f50a4d2877096b6882))
* hbase 2x shell (bigtable2.0) ([#2906](https://www.github.com/googleapis/java-bigtable-hbase/issues/2906)) ([eb9e756](https://www.github.com/googleapis/java-bigtable-hbase/commit/eb9e7563911105d00f729263ae520e376cabdaa2))
* includes fixes for the gap between HBase and this client ([#2267](https://www.github.com/googleapis/java-bigtable-hbase/issues/2267)) ([51f0617](https://www.github.com/googleapis/java-bigtable-hbase/commit/51f0617aa8c9865b066e9bfaa25f053c7f0ad3d4))
* keep only failed actions in List<Delete> ([#3020](https://www.github.com/googleapis/java-bigtable-hbase/issues/3020)) ([aac0522](https://www.github.com/googleapis/java-bigtable-hbase/commit/aac0522b071c065e3d41ad1c4d1d8498516f9311))
* **logs:** type aligned to in log statement ([#2536](https://www.github.com/googleapis/java-bigtable-hbase/issues/2536)) ([76c41ca](https://www.github.com/googleapis/java-bigtable-hbase/commit/76c41ca37986cc703a27af971012949c425fb329))
* manifests in executable jars ([#2896](https://www.github.com/googleapis/java-bigtable-hbase/issues/2896)) ([9736b06](https://www.github.com/googleapis/java-bigtable-hbase/commit/9736b063dcd5c54084f6f08a284c28410388b8bb))
* max mutation comparison to include 100k ([#3008](https://www.github.com/googleapis/java-bigtable-hbase/issues/3008)) ([5434e5e](https://www.github.com/googleapis/java-bigtable-hbase/commit/5434e5ee24ff69f81aa8f4ee96713346ee6f9d86))
* multithreaded batch operations ([#2930](https://www.github.com/googleapis/java-bigtable-hbase/issues/2930)) ([8c1eb6c](https://www.github.com/googleapis/java-bigtable-hbase/commit/8c1eb6c5624357603a2e6f8ffcc277384390643c))
* qualifier filter comparators ([#2684](https://www.github.com/googleapis/java-bigtable-hbase/issues/2684)) ([#2688](https://www.github.com/googleapis/java-bigtable-hbase/issues/2688)) ([f57affc](https://www.github.com/googleapis/java-bigtable-hbase/commit/f57affce41a14efbad2a1588c744eb706ae4d50b))
* race condition where a retry triggers a full table scan ([#2758](https://www.github.com/googleapis/java-bigtable-hbase/issues/2758)) ([8d3bd4d](https://www.github.com/googleapis/java-bigtable-hbase/commit/8d3bd4dcd5dd85b2521d983b7bcd306e680264c2))
* ReadRows not counting the rpc as the first attempt ([#2568](https://www.github.com/googleapis/java-bigtable-hbase/issues/2568)) ([c748f23](https://www.github.com/googleapis/java-bigtable-hbase/commit/c748f23b60f83d6468fcb52ac5477b74a3bab964))
* Refactoring methods in TemplateUtils. Using CamelCasing for names. ([#2967](https://www.github.com/googleapis/java-bigtable-hbase/issues/2967)) ([93a1c2d](https://www.github.com/googleapis/java-bigtable-hbase/commit/93a1c2de2895df99ccda8d49c136fa8661a2e91d))
* remove duplicate classic setting parsing ([#2918](https://www.github.com/googleapis/java-bigtable-hbase/issues/2918)) ([b6b44a7](https://www.github.com/googleapis/java-bigtable-hbase/commit/b6b44a7c390513322bc9e976556360f58c9e57d0))
* remove duplicated cell when interleave filter is applied ([#2491](https://www.github.com/googleapis/java-bigtable-hbase/issues/2491)) ([2915bfd](https://www.github.com/googleapis/java-bigtable-hbase/commit/2915bfd5527bd6beabab264a79fa764f2e6a7629))
* retry rst_stream ([#3002](https://www.github.com/googleapis/java-bigtable-hbase/issues/3002)) ([ace17b7](https://www.github.com/googleapis/java-bigtable-hbase/commit/ace17b76a21b859c421049fc8f0d927c86a065f7))
* Set a 20 mins timeout for bulk mutations for HBase over Veneer. ([#3052](https://www.github.com/googleapis/java-bigtable-hbase/issues/3052)) ([10a6ce8](https://www.github.com/googleapis/java-bigtable-hbase/commit/10a6ce8b752640792ebe26d66e2523cb9a287dd7))
* temporarily disable reporting to unblock releases ([#2620](https://www.github.com/googleapis/java-bigtable-hbase/issues/2620)) ([9611a91](https://www.github.com/googleapis/java-bigtable-hbase/commit/9611a912c8a61fe9cd78ad49fdb71d9051b7accc))
* the options of CreateTableHelper to be public ([#2366](https://www.github.com/googleapis/java-bigtable-hbase/issues/2366)) ([072ccc9](https://www.github.com/googleapis/java-bigtable-hbase/commit/072ccc9a0bd16efb5c4dab5900447a3d2789bac8))
* to fix deleteRowRangeByPrefix for integer values above 127 ([#2511](https://www.github.com/googleapis/java-bigtable-hbase/issues/2511)) ([1ae8c03](https://www.github.com/googleapis/java-bigtable-hbase/commit/1ae8c03f1636b18c3fb6310db61a30f6ab7e9646))
* update readRowsAsync to use RetryingReadRowsOperation ([#2738](https://www.github.com/googleapis/java-bigtable-hbase/issues/2738)) ([#2759](https://www.github.com/googleapis/java-bigtable-hbase/issues/2759)) ([3c4f3b2](https://www.github.com/googleapis/java-bigtable-hbase/commit/3c4f3b27c0abfd65fe42ebbdf4827c9c41feab1d))
* update url in pom file ([#2279](https://www.github.com/googleapis/java-bigtable-hbase/issues/2279)) ([2e2f638](https://www.github.com/googleapis/java-bigtable-hbase/commit/2e2f638add2e4eb69e6b241c98f2f420527396ab))
* updated assertions and scan for firstKeyOnlyFilter test ([#2483](https://www.github.com/googleapis/java-bigtable-hbase/issues/2483)) ([a2cbe7a](https://www.github.com/googleapis/java-bigtable-hbase/commit/a2cbe7a97c2f65bd1f2a21eaba0c8868b315d55d))
* use binary search in getRegionLocation ([#3044](https://www.github.com/googleapis/java-bigtable-hbase/issues/3044)) ([48951ab](https://www.github.com/googleapis/java-bigtable-hbase/commit/48951abde05441d78d87b76c4a34285b2dd80561))
* use random with permanent seed ([#2722](https://www.github.com/googleapis/java-bigtable-hbase/issues/2722)) ([a882f9a](https://www.github.com/googleapis/java-bigtable-hbase/commit/a882f9a5cef1c46ff4d16ba3b5b9082a69c640d1))
* validation of TimeRange ([#1890](https://www.github.com/googleapis/java-bigtable-hbase/issues/1890)) ([7f404e4](https://www.github.com/googleapis/java-bigtable-hbase/commit/7f404e4f21004f8b552f4c5b80d409496f4a7240))
* veneer adapter batching ([#3053](https://www.github.com/googleapis/java-bigtable-hbase/issues/3053)) ([6768357](https://www.github.com/googleapis/java-bigtable-hbase/commit/6768357f755e9fd117faf88c3456fcc89384300c))
* ZeroCopyByteStringUtil can return incorrect results in some cases ([#2628](https://www.github.com/googleapis/java-bigtable-hbase/issues/2628)) ([1667a40](https://www.github.com/googleapis/java-bigtable-hbase/commit/1667a403fd09b8768ef2411059733feec64da3e9))


### Reverts

* Revert "fix: BigtableSession is never closed by Reader causing "ManagedChanne… ([#2782](https://www.github.com/googleapis/java-bigtable-hbase/issues/2782))" ([#2873](https://www.github.com/googleapis/java-bigtable-hbase/issues/2873)) ([3568c27](https://www.github.com/googleapis/java-bigtable-hbase/commit/3568c27c764c853a0f57cbc6066ac5d95010c4e8))


### Miscellaneous Chores

* bump release please to 2.0.0-alpha-1 ([#2927](https://www.github.com/googleapis/java-bigtable-hbase/issues/2927)) ([7b0edcd](https://www.github.com/googleapis/java-bigtable-hbase/commit/7b0edcd91ad5c30950f9c7c044b19bf4575ad27a))
* make release please version 2.0.0-alpha1 ([#3155](https://www.github.com/googleapis/java-bigtable-hbase/issues/3155)) ([890b679](https://www.github.com/googleapis/java-bigtable-hbase/commit/890b679d75a977873d9c8cd6d1578798b6e92e28))


### Documentation

* add javadoc for classes marked as InternalApi ([#2350](https://www.github.com/googleapis/java-bigtable-hbase/issues/2350)) ([c86d7d2](https://www.github.com/googleapis/java-bigtable-hbase/commit/c86d7d265b2816136e3d6313eeb8fa87d74f9635))
* Add README for HBase Tools and Beam import/export and validator pipelines ([#2949](https://www.github.com/googleapis/java-bigtable-hbase/issues/2949)) ([e05b548](https://www.github.com/googleapis/java-bigtable-hbase/commit/e05b5482b97a71227078a47244114c8b94e3f7d2))
* automatically update version numbers with release ([#2476](https://www.github.com/googleapis/java-bigtable-hbase/issues/2476)) ([2ad78e9](https://www.github.com/googleapis/java-bigtable-hbase/commit/2ad78e919fada035e1c6d92f056c8dbf64771f4a))
* Fix broken links for HBase Migration tools ([#3097](https://www.github.com/googleapis/java-bigtable-hbase/issues/3097)) ([83238e7](https://www.github.com/googleapis/java-bigtable-hbase/commit/83238e74826d0948fea6edd27f5ceee761cc409c))
* fix link to 1x branch in readme ([#3161](https://www.github.com/googleapis/java-bigtable-hbase/issues/3161)) ([b4e1757](https://www.github.com/googleapis/java-bigtable-hbase/commit/b4e17573baf40877515851174979a374e8b8c505))
* fix readme title for Bigtable HBase tools ([#3013](https://www.github.com/googleapis/java-bigtable-hbase/issues/3013)) ([de5970d](https://www.github.com/googleapis/java-bigtable-hbase/commit/de5970df25c3b0590a7a50ae833b1256877609bd))
* update CONTRIBUTING.md to include code formatting ([#534](https://www.github.com/googleapis/java-bigtable-hbase/issues/534)) ([#2542](https://www.github.com/googleapis/java-bigtable-hbase/issues/2542)) ([1c92056](https://www.github.com/googleapis/java-bigtable-hbase/commit/1c920563edd114589ff6896f396a0a2d021fd698))
* Update CONTRIBUTING.md with integration test instructions ([#2560](https://www.github.com/googleapis/java-bigtable-hbase/issues/2560)) ([9b1a6e5](https://www.github.com/googleapis/java-bigtable-hbase/commit/9b1a6e5738dd0362be8a12a2cf18f623015f5243))
* update readme to align more with standard templates ([#3162](https://www.github.com/googleapis/java-bigtable-hbase/issues/3162)) ([1721ae1](https://www.github.com/googleapis/java-bigtable-hbase/commit/1721ae17370ccd0a6c35aaae48f4ff9151d794d3))
* Updated readme to redirect user to veneer client ([#2288](https://www.github.com/googleapis/java-bigtable-hbase/issues/2288)) ([aa9ac47](https://www.github.com/googleapis/java-bigtable-hbase/commit/aa9ac475ca6633dd38d338192ca9ddae7a423188))


### Dependencies

* add maven-enforcer-plugin ([#2401](https://www.github.com/googleapis/java-bigtable-hbase/issues/2401)) ([1ef4b9c](https://www.github.com/googleapis/java-bigtable-hbase/commit/1ef4b9cb23a11d630114f382080baac66fa2f26d))
* align bigtable-hbase-1.x-mapreduce to use the same hadoop version as other 1.x modules ([#2455](https://www.github.com/googleapis/java-bigtable-hbase/issues/2455)) ([60a5f82](https://www.github.com/googleapis/java-bigtable-hbase/commit/60a5f82e3e3c9affdcaf5ae2a8b79c433b5a1a1b))
* fix dependency build ([#2305](https://www.github.com/googleapis/java-bigtable-hbase/issues/2305)) ([e92fd2e](https://www.github.com/googleapis/java-bigtable-hbase/commit/e92fd2e560421059bda83a0c0fd2f11f73ba9fcd))
* fix hbaseLocalMiniClusterTestH2 tests ([#2308](https://www.github.com/googleapis/java-bigtable-hbase/issues/2308)) ([b7f48f5](https://www.github.com/googleapis/java-bigtable-hbase/commit/b7f48f5d7844af2e5466c8be3437edd7fe05e152))
* fix undeclared used dependencies ([#2419](https://www.github.com/googleapis/java-bigtable-hbase/issues/2419)) ([d9b109f](https://www.github.com/googleapis/java-bigtable-hbase/commit/d9b109f01b53924476b0d2b8cdbe5b36ef83bdad))
* **fix:** add log4j12 to mincluster h2 tests ([#2450](https://www.github.com/googleapis/java-bigtable-hbase/issues/2450)) ([bf5ef7c](https://www.github.com/googleapis/java-bigtable-hbase/commit/bf5ef7c80dd504baa56887fbdfe975677c05ab34))
* manually update dependency org.mockito:mockito-core to v3.3.0 ([#2392](https://www.github.com/googleapis/java-bigtable-hbase/issues/2392)) ([918340d](https://www.github.com/googleapis/java-bigtable-hbase/commit/918340dfe3abf60411624fee6b6aa826644e24d9))
* Update beam version to  2.24.0 ([#2775](https://www.github.com/googleapis/java-bigtable-hbase/issues/2775)) ([82ca972](https://www.github.com/googleapis/java-bigtable-hbase/commit/82ca9721bf1a41025e40b056e32b0a24748e9c0e))
* update bigtable veneer to 1.15.0 ([#2630](https://www.github.com/googleapis/java-bigtable-hbase/issues/2630)) ([9034822](https://www.github.com/googleapis/java-bigtable-hbase/commit/90348227215039ce0370e67db7b4d0b0daf7b1ec))
* update bigtable veneer version to 1.12.2 ([#2526](https://www.github.com/googleapis/java-bigtable-hbase/issues/2526)) ([c422b07](https://www.github.com/googleapis/java-bigtable-hbase/commit/c422b07f06cb55831e2287fd6dced7ce46ea25da))
* update bigtable.version to v1.11.0 ([#2395](https://www.github.com/googleapis/java-bigtable-hbase/issues/2395)) ([50de5d7](https://www.github.com/googleapis/java-bigtable-hbase/commit/50de5d76520485ec8e13a0b481a4b88dcd3fd56c))
* update bigtable.version to v1.13.0 ([#2540](https://www.github.com/googleapis/java-bigtable-hbase/issues/2540)) ([2167870](https://www.github.com/googleapis/java-bigtable-hbase/commit/21678704f17cc5487bb280e6be56e5cd26a3a9bc))
* update bigtable.version to v1.16.1 ([#2646](https://www.github.com/googleapis/java-bigtable-hbase/issues/2646)) ([a48a456](https://www.github.com/googleapis/java-bigtable-hbase/commit/a48a45617db58872797d4384c714463403ec5eeb))
* update bigtable.version to v1.16.2 ([#2654](https://www.github.com/googleapis/java-bigtable-hbase/issues/2654)) ([ad48fb1](https://www.github.com/googleapis/java-bigtable-hbase/commit/ad48fb14f97b696727ae6dfce7963f13770227a1))
* update bigtable.version to v1.17.0 ([#2668](https://www.github.com/googleapis/java-bigtable-hbase/issues/2668)) ([15a2f39](https://www.github.com/googleapis/java-bigtable-hbase/commit/15a2f39f51389187af78a4441e78c743cc080846))
* update bigtable.version to v1.17.1 ([#2674](https://www.github.com/googleapis/java-bigtable-hbase/issues/2674)) ([7ee5f0a](https://www.github.com/googleapis/java-bigtable-hbase/commit/7ee5f0a3851bf83ef371446f589ccfd30017299f))
* update bigtable.version to v1.17.3 ([#2695](https://www.github.com/googleapis/java-bigtable-hbase/issues/2695)) ([75cf25c](https://www.github.com/googleapis/java-bigtable-hbase/commit/75cf25cded78a0a3ecb1315f0f1153c8c9cf20c3))
* update bigtable.version to v1.18.0 ([#2700](https://www.github.com/googleapis/java-bigtable-hbase/issues/2700)) ([48a60f6](https://www.github.com/googleapis/java-bigtable-hbase/commit/48a60f6ecb2451b8fd400d69703daab139a60e4c))
* update bigtable.version to v1.19.0 ([#2721](https://www.github.com/googleapis/java-bigtable-hbase/issues/2721)) ([b813bd4](https://www.github.com/googleapis/java-bigtable-hbase/commit/b813bd48d56048fe9252f3ff7692fb1275baa775))
* update bigtable.version to v1.19.1 ([#2763](https://www.github.com/googleapis/java-bigtable-hbase/issues/2763)) ([b5d9ec2](https://www.github.com/googleapis/java-bigtable-hbase/commit/b5d9ec29f2b3805ab868738e67270c3bd79d5d03))
* update bigtable.version to v1.19.2 ([#2767](https://www.github.com/googleapis/java-bigtable-hbase/issues/2767)) ([792f7f5](https://www.github.com/googleapis/java-bigtable-hbase/commit/792f7f56f064f78bde70c5b1d62cedf4e936c553))
* update bigtable.version to v1.20.0 ([#2829](https://www.github.com/googleapis/java-bigtable-hbase/issues/2829)) ([098d370](https://www.github.com/googleapis/java-bigtable-hbase/commit/098d3705e1de445efc975f5ce1504096be05f24e))
* update bigtable.version to v1.20.1 ([#2843](https://www.github.com/googleapis/java-bigtable-hbase/issues/2843)) ([c4126da](https://www.github.com/googleapis/java-bigtable-hbase/commit/c4126da6b354c62375e9c35b574ca737da7bcc2e))
* update bigtable.version to v1.21.0 ([#2868](https://www.github.com/googleapis/java-bigtable-hbase/issues/2868)) ([8fbf496](https://www.github.com/googleapis/java-bigtable-hbase/commit/8fbf496f95853bf9fbe3107c547878e4e94665c9))
* update dependency com.fasterxml.jackson.core:jackson-databind to v2.10.2 ([#2340](https://www.github.com/googleapis/java-bigtable-hbase/issues/2340)) ([437557f](https://www.github.com/googleapis/java-bigtable-hbase/commit/437557fd108db51273dd97d9d0eb1c9bfeecda6d))
* update dependency com.fasterxml.jackson.core:jackson-databind to v2.10.3 ([#2394](https://www.github.com/googleapis/java-bigtable-hbase/issues/2394)) ([95834d1](https://www.github.com/googleapis/java-bigtable-hbase/commit/95834d1623546c028c347ede6fe1f0169a66de00))
* update dependency com.fasterxml.jackson.core:jackson-databind to v2.11.0 ([#2505](https://www.github.com/googleapis/java-bigtable-hbase/issues/2505)) ([dd8856f](https://www.github.com/googleapis/java-bigtable-hbase/commit/dd8856f81b64249b8a9da28f0aa9350fa9887b4a))
* update dependency com.fasterxml.jackson.core:jackson-databind to v2.11.1 ([#2557](https://www.github.com/googleapis/java-bigtable-hbase/issues/2557)) ([65373ce](https://www.github.com/googleapis/java-bigtable-hbase/commit/65373ced9dd22050ce464c285d1f5d5d70f1b76d))
* update dependency com.fasterxml.jackson.core:jackson-databind to v2.11.2 ([#2603](https://www.github.com/googleapis/java-bigtable-hbase/issues/2603)) ([046affa](https://www.github.com/googleapis/java-bigtable-hbase/commit/046affab81031e616fb3f3500f7d8bc379f55c16))
* update dependency com.google.auto.value:auto-value to v1.7.2 ([#2513](https://www.github.com/googleapis/java-bigtable-hbase/issues/2513)) ([409b309](https://www.github.com/googleapis/java-bigtable-hbase/commit/409b3094846e98cbc3286057ac98cba2f3332339))
* update dependency com.google.auto.value:auto-value to v1.7.3 ([#2549](https://www.github.com/googleapis/java-bigtable-hbase/issues/2549)) ([1161524](https://www.github.com/googleapis/java-bigtable-hbase/commit/1161524ecc3335d0e824bc7045159319f9885a83))
* update dependency com.google.auto.value:auto-value to v1.7.4 ([#2571](https://www.github.com/googleapis/java-bigtable-hbase/issues/2571)) ([4dd36a7](https://www.github.com/googleapis/java-bigtable-hbase/commit/4dd36a732ce229508f2d49dcb09ed36fe0f6ede6))
* update dependency com.google.auto.value:auto-value-annotations to v1.7.2 ([#2532](https://www.github.com/googleapis/java-bigtable-hbase/issues/2532)) ([818f435](https://www.github.com/googleapis/java-bigtable-hbase/commit/818f4354846b848307b74c4d927d73833da254e6))
* update dependency com.google.auto.value:auto-value-annotations to v1.7.3 ([#2550](https://www.github.com/googleapis/java-bigtable-hbase/issues/2550)) ([218bcbe](https://www.github.com/googleapis/java-bigtable-hbase/commit/218bcbebecfc99ccc3a2b26d817c904e967daa3c))
* update dependency com.google.auto.value:auto-value-annotations to v1.7.4 ([#2572](https://www.github.com/googleapis/java-bigtable-hbase/issues/2572)) ([bf79eaf](https://www.github.com/googleapis/java-bigtable-hbase/commit/bf79eaf6e229f42b4b74cf46500e06ebf932239b))
* update dependency com.google.cloud:google-cloud-bigtable-emulator to v0.129.0 ([#2832](https://www.github.com/googleapis/java-bigtable-hbase/issues/2832)) ([4a6b1f4](https://www.github.com/googleapis/java-bigtable-hbase/commit/4a6b1f4711ac4f36eb9fda42a480842eab2aa0e5))
* update dependency com.google.cloud:google-cloud-bigtable-emulator to v0.129.1 ([#2844](https://www.github.com/googleapis/java-bigtable-hbase/issues/2844)) ([abcdcc3](https://www.github.com/googleapis/java-bigtable-hbase/commit/abcdcc30c0ede9f3b8ecf9182a82487d006d682e))
* update dependency com.google.cloud:google-cloud-bigtable-emulator to v0.130.0 ([#2869](https://www.github.com/googleapis/java-bigtable-hbase/issues/2869)) ([207be9b](https://www.github.com/googleapis/java-bigtable-hbase/commit/207be9b688fa9b747ff76ae73ae8ef9e7c1d178d))
* update dependency com.google.guava:guava to v30 ([#2666](https://www.github.com/googleapis/java-bigtable-hbase/issues/2666)) ([8289a54](https://www.github.com/googleapis/java-bigtable-hbase/commit/8289a543df878a2cb861c9084fa160a35bbc1d41))
* update dependency com.google.guava:guava to v30.1-android ([#2761](https://www.github.com/googleapis/java-bigtable-hbase/issues/2761)) ([07f263e](https://www.github.com/googleapis/java-bigtable-hbase/commit/07f263e6895c6c56cc1ed11e6882b7ad0fa546cd))
* update dependency com.google.http-client:google-http-client-jackson2 to v1.35.0 ([#2507](https://www.github.com/googleapis/java-bigtable-hbase/issues/2507)) ([73f5c5e](https://www.github.com/googleapis/java-bigtable-hbase/commit/73f5c5e6ee599db681e3c27bc96fe0664db7d45e))
* update dependency com.google.http-client:google-http-client-jackson2 to v1.36.0 ([#2559](https://www.github.com/googleapis/java-bigtable-hbase/issues/2559)) ([5567896](https://www.github.com/googleapis/java-bigtable-hbase/commit/55678969c0cf1a7ed74f34c95caef0ed2bf8291e))
* update dependency com.google.http-client:google-http-client-jackson2 to v1.37.0 ([#2652](https://www.github.com/googleapis/java-bigtable-hbase/issues/2652)) ([64ea0e1](https://www.github.com/googleapis/java-bigtable-hbase/commit/64ea0e150a25b3f48de651d941dba93718a48166))
* update dependency com.google.http-client:google-http-client-jackson2 to v1.38.0 ([#2675](https://www.github.com/googleapis/java-bigtable-hbase/issues/2675)) ([95cde54](https://www.github.com/googleapis/java-bigtable-hbase/commit/95cde54a2ccb7ef0128f7ab6696d77b27ab465ed))
* update dependency com.google.http-client:google-http-client-jackson2 to v1.38.1 ([#2802](https://www.github.com/googleapis/java-bigtable-hbase/issues/2802)) ([442e279](https://www.github.com/googleapis/java-bigtable-hbase/commit/442e279fd11becdeac2a53a9edcc4dffc3ea6d69))
* update dependency com.google.http-client:google-http-client-jackson2 to v1.39.0 ([#2848](https://www.github.com/googleapis/java-bigtable-hbase/issues/2848)) ([6da8736](https://www.github.com/googleapis/java-bigtable-hbase/commit/6da873671cb1cdab85e4fd4514ba704fc8d88c34))
* update dependency commons-codec:commons-codec to v1.14 ([#2339](https://www.github.com/googleapis/java-bigtable-hbase/issues/2339)) ([41a3d98](https://www.github.com/googleapis/java-bigtable-hbase/commit/41a3d98cb4561917ac91e1ea9207b99e1f3e0067))
* update dependency commons-codec:commons-codec to v1.15 ([#2636](https://www.github.com/googleapis/java-bigtable-hbase/issues/2636)) ([e604c3e](https://www.github.com/googleapis/java-bigtable-hbase/commit/e604c3e04fdd82add41ef3882dd6630b002e6b18))
* update dependency io.grpc:grpc-bom to v1.26.0 ([6c616c9](https://www.github.com/googleapis/java-bigtable-hbase/commit/6c616c90d1e4c280b62a74e37bd84cdb6b7ec76e))
* update dependency junit:junit to v4.13 ([#2335](https://www.github.com/googleapis/java-bigtable-hbase/issues/2335)) ([92537f0](https://www.github.com/googleapis/java-bigtable-hbase/commit/92537f088691ea10de87ef6a580ae8f22c323132))
* update dependency org.apache.commons:commons-compress to v1.20 ([#2376](https://www.github.com/googleapis/java-bigtable-hbase/issues/2376)) ([65ac9cd](https://www.github.com/googleapis/java-bigtable-hbase/commit/65ac9cdfd7a45fcf663dd49f79896494ea993ea2))
* update dependency org.codehaus.plexus:plexus-utils to v3.3.0 ([#2860](https://www.github.com/googleapis/java-bigtable-hbase/issues/2860)) ([c6bf393](https://www.github.com/googleapis/java-bigtable-hbase/commit/c6bf3932c97024ac01734bcee0c2ac4c55765890))
* update dependency org.mockito:mockito-core to v3.2.4 ([#2290](https://www.github.com/googleapis/java-bigtable-hbase/issues/2290)) ([2e2e4f9](https://www.github.com/googleapis/java-bigtable-hbase/commit/2e2e4f9d6df349232e16f4bebe184de973edf8d2))
* update dependency org.mockito:mockito-core to v3.3.3 ([#2414](https://www.github.com/googleapis/java-bigtable-hbase/issues/2414)) ([e82bc10](https://www.github.com/googleapis/java-bigtable-hbase/commit/e82bc102f25085f3c223073d5e142f3376b0ec55))
* update dependency org.mockito:mockito-core to v3.4.0 ([#2578](https://www.github.com/googleapis/java-bigtable-hbase/issues/2578)) ([d6a351e](https://www.github.com/googleapis/java-bigtable-hbase/commit/d6a351ecac04b58fd7b3706ff074d4af65212121))
* update dependency org.mockito:mockito-core to v3.4.6 ([#2593](https://www.github.com/googleapis/java-bigtable-hbase/issues/2593)) ([6fe4fc1](https://www.github.com/googleapis/java-bigtable-hbase/commit/6fe4fc1a306b23509e38ac7ddbd40248389c89fc))
* update dependency org.mockito:mockito-core to v3.5.13 ([#2638](https://www.github.com/googleapis/java-bigtable-hbase/issues/2638)) ([47741d7](https://www.github.com/googleapis/java-bigtable-hbase/commit/47741d71b8f0a74b007430974c7565f2afe54d95))
* update dependency org.mockito:mockito-core to v3.5.15 ([#2660](https://www.github.com/googleapis/java-bigtable-hbase/issues/2660)) ([500eed1](https://www.github.com/googleapis/java-bigtable-hbase/commit/500eed1ef0bb52dc90f170e8675ac21b5cb2d872))
* update dependency org.mockito:mockito-core to v3.5.7 ([#2609](https://www.github.com/googleapis/java-bigtable-hbase/issues/2609)) ([7c6c3c8](https://www.github.com/googleapis/java-bigtable-hbase/commit/7c6c3c82450fa76135d93ab8f78de3c14da2debc))
* update dependency org.slf4j:slf4j-api to v1.7.30 ([#2309](https://www.github.com/googleapis/java-bigtable-hbase/issues/2309)) ([8182b5d](https://www.github.com/googleapis/java-bigtable-hbase/commit/8182b5d3e492713738ef2f8589ef714c2cabfa5b))
* update hbase1-hadoop.version to v2.10.1 ([#2575](https://www.github.com/googleapis/java-bigtable-hbase/issues/2575)) ([486589b](https://www.github.com/googleapis/java-bigtable-hbase/commit/486589b9361bf0e62b466b332bbc23964ffe4d0c))
* update jmh.version to v1.22 ([#2348](https://www.github.com/googleapis/java-bigtable-hbase/issues/2348)) ([cdec57f](https://www.github.com/googleapis/java-bigtable-hbase/commit/cdec57fc7b10bbea62c8f7def6ada389849aa970))
* update jmh.version to v1.23 ([#2370](https://www.github.com/googleapis/java-bigtable-hbase/issues/2370)) ([4959c8f](https://www.github.com/googleapis/java-bigtable-hbase/commit/4959c8f0ca119e19a7585875ba056934dda60957))
* update jmh.version to v1.27 ([#2739](https://www.github.com/googleapis/java-bigtable-hbase/issues/2739)) ([b6bf3c7](https://www.github.com/googleapis/java-bigtable-hbase/commit/b6bf3c747ad1e401957a2fa76177efbc8cd221ea))
* update jmh.version to v1.28 ([#2853](https://www.github.com/googleapis/java-bigtable-hbase/issues/2853)) ([ad7a80b](https://www.github.com/googleapis/java-bigtable-hbase/commit/ad7a80be355497c19d985fca55c3845042638ad2))
* update shared config to 0.9.2 ([#2635](https://www.github.com/googleapis/java-bigtable-hbase/issues/2635)) ([fcdde22](https://www.github.com/googleapis/java-bigtable-hbase/commit/fcdde22099efdc0451cd4872b128169b4f61ea7a))
* upgrade veneer to 1.27.2 ([#3056](https://www.github.com/googleapis/java-bigtable-hbase/issues/3056)) ([5c63d34](https://www.github.com/googleapis/java-bigtable-hbase/commit/5c63d34ff25bf5ba4371f638080b9392a565ec6c))
* upgrade veneer to 1.27.3 ([#3164](https://www.github.com/googleapis/java-bigtable-hbase/issues/3164)) ([053398c](https://www.github.com/googleapis/java-bigtable-hbase/commit/053398cee434dd6f96e2b78508fc468b3004458a))
