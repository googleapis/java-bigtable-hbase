# Changelog

## [1.29.2](https://github.com/googleapis/java-bigtable-hbase/compare/v1.29.1...v1.29.2) (2023-03-27)


### Bug Fixes

* Dropwizard metrics probing ([#4032](https://github.com/googleapis/java-bigtable-hbase/issues/4032)) ([77bf060](https://github.com/googleapis/java-bigtable-hbase/commit/77bf0608fff88efff161ae70f064dda3915dbadd))

## [1.29.1](https://github.com/googleapis/java-bigtable-hbase/compare/v1.29.0...v1.29.1) (2023-03-24)


### Bug Fixes

* Fix dropwizard probing ([e2ff8fd](https://github.com/googleapis/java-bigtable-hbase/commit/e2ff8fd87f1d3efa531dde1eefbcf18fa3421fd5))

## [1.29.0](https://github.com/googleapis/java-bigtable-hbase/compare/v1.28.0...v1.29.0) (2023-03-21)


### Features

* Make dropwizard metrics-core optional ([#4012](https://github.com/googleapis/java-bigtable-hbase/issues/4012)) ([fca9cde](https://github.com/googleapis/java-bigtable-hbase/commit/fca9cde7f69338b4324032b3a5fbe14e045fc7f0))

## [1.28.0](https://github.com/googleapis/java-bigtable-hbase/compare/v1.27.1...v1.28.0) (2022-10-28)


### Features

* relocate bigtable-client-core configuration to bigtable-client-core-config ([3d0c2a5](https://github.com/googleapis/java-bigtable-hbase/commit/3d0c2a548e6d4f18f9972d873332f8f0b6db0719))

## [1.27.1](https://github.com/googleapis/java-bigtable-hbase/compare/v1.27.0...v1.27.1) (2022-08-01)


### Bug Fixes

* Add an exception interceptor to convert RST_STREAM errors to unavailable ([#3701](https://github.com/googleapis/java-bigtable-hbase/issues/3701)) ([8fadf5e](https://github.com/googleapis/java-bigtable-hbase/commit/8fadf5e957e981b39f6f7930597af6260fbdb59f))

## [1.27.0](https://github.com/googleapis/java-bigtable-hbase/compare/v1.26.3...v1.27.0) (2022-07-08)


### Features

* populate x-goog-params with table name and app profile id ([#3677](https://github.com/googleapis/java-bigtable-hbase/issues/3677)) ([5c2a11d](https://github.com/googleapis/java-bigtable-hbase/commit/5c2a11d46e37512df260b9484cfe2524b4afce62))

### [1.26.3](https://github.com/googleapis/java-bigtable-hbase/compare/v1.26.2...v1.26.3) (2022-01-18)


### Dependencies

* upgrade protobuf to 3.19.2 ([#3455](https://github.com/googleapis/java-bigtable-hbase/issues/3455)) ([a9bec1f](https://github.com/googleapis/java-bigtable-hbase/commit/a9bec1ff3df997be3b2f3eb773c6070b13f2626e))

### [1.26.2](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.26.1...v1.26.2) (2022-01-05)


### Dependencies

* log4j 2.17.0 ([#3405](https://www.github.com/googleapis/java-bigtable-hbase/issues/3405)) ([0675ffb](https://www.github.com/googleapis/java-bigtable-hbase/commit/0675ffbaf1d47346034bafe23867e27f353dc52f))
* log4j 2.17.1 ([#3422](https://www.github.com/googleapis/java-bigtable-hbase/issues/3422)) ([d78fc91](https://www.github.com/googleapis/java-bigtable-hbase/commit/d78fc9165fa9b4dbb9c546ce8bdbdc26e4bfff07))
* migrate to logback-classic (port of [#3425](https://www.github.com/googleapis/java-bigtable-hbase/issues/3425)) ([#3426](https://www.github.com/googleapis/java-bigtable-hbase/issues/3426)) ([8122404](https://www.github.com/googleapis/java-bigtable-hbase/commit/81224043c929c0f629e317bdb77bb80e3cf95c3b))

### [1.26.1](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.26.0...v1.26.1) (2021-12-14)


### Dependencies

* **fix:** update to 2.16.0 log4j version and ban all 2.x versions which are <= 2.15.0 ([#3380](https://www.github.com/googleapis/java-bigtable-hbase/issues/3380)) ([3e3fb6c](https://www.github.com/googleapis/java-bigtable-hbase/commit/3e3fb6cb6837b86ac0f053ce263b6b364faddb13))

## [1.26.0](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.25.1...v1.26.0) (2021-12-10)


### Features

* bump next 1.x release to 1.26.0 ([#3327](https://www.github.com/googleapis/java-bigtable-hbase/issues/3327)) ([7da8fc2](https://www.github.com/googleapis/java-bigtable-hbase/commit/7da8fc24b29c0b9f01eda960630018421e2b1ee9))


### Dependencies

* migrate to log4j-core ([#3326](https://www.github.com/googleapis/java-bigtable-hbase/issues/3326)) ([#3375](https://www.github.com/googleapis/java-bigtable-hbase/issues/3375)) ([9142f13](https://www.github.com/googleapis/java-bigtable-hbase/commit/9142f1341f4542c8a48976cc3cf4675f76fe05db))

### [1.25.1](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.25.0...v1.25.1) (2021-10-13)


### Bug Fixes

* fix rst stream error matching ([#3275](https://www.github.com/googleapis/java-bigtable-hbase/issues/3275)) ([0fce614](https://www.github.com/googleapis/java-bigtable-hbase/commit/0fce614e4b1b128ae565271845ffb96e972a8546))

## [1.25.0](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.24.0...v1.25.0) (2021-09-24)


### Features

* migrate to google-auth-library for pre-emptive oauth refresh ([#3228](https://www.github.com/googleapis/java-bigtable-hbase/issues/3228)) ([8031a29](https://www.github.com/googleapis/java-bigtable-hbase/commit/8031a297aaf7bf612862b21efb70962ad1966633))


### Bug Fixes

* adding validation for snapshot name for hbase import pipeline ([#3203](https://www.github.com/googleapis/java-bigtable-hbase/issues/3203)) ([#3226](https://www.github.com/googleapis/java-bigtable-hbase/issues/3226)) ([38925e8](https://www.github.com/googleapis/java-bigtable-hbase/commit/38925e8517403294b0958296aa5f20da6ba5cc55))


### Documentation

* Add instructions for migrating from HBase to Bigtable (offline via snapshots) ([#3197](https://www.github.com/googleapis/java-bigtable-hbase/issues/3197)) ([17bda3a](https://www.github.com/googleapis/java-bigtable-hbase/commit/17bda3a2d65d8d154b68dc3ff6c538544390ffd5))

## [1.24.0](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.23.1...v1.24.0) (2021-08-31)


### Features

* Add support for renaming tables in schema translator ([#3185](https://www.github.com/googleapis/java-bigtable-hbase/issues/3185)) ([e294c1e](https://www.github.com/googleapis/java-bigtable-hbase/commit/e294c1ecd9a985558e8447cf2fa954040016a23a))
* log bulk mutation entry errors ([#3198](https://www.github.com/googleapis/java-bigtable-hbase/issues/3198)) ([0618ddb](https://www.github.com/googleapis/java-bigtable-hbase/commit/0618ddb6a323c3d795658a306721751502329fcd))

### [1.23.1](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.23.0...v1.23.1) (2021-08-09)


### Bug Fixes

* allow direct path to be used with custom endpoints ([#3184](https://www.github.com/googleapis/java-bigtable-hbase/issues/3184)) ([7178129](https://www.github.com/googleapis/java-bigtable-hbase/commit/7178129e76275caa8339874577e5c25c7d78737c))


### Dependencies

* upgrade commons-compress to 1.21 ([#3174](https://www.github.com/googleapis/java-bigtable-hbase/issues/3174)) ([26435e5](https://www.github.com/googleapis/java-bigtable-hbase/commit/26435e51fa14ce97e0140028a7fc427e2612a83e))

## [1.23.0](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.22.0...v1.23.0) (2021-07-14)


### Features

* update default settings to match java-bigtable ([#3151](https://www.github.com/googleapis/java-bigtable-hbase/issues/3151)) ([712c650](https://www.github.com/googleapis/java-bigtable-hbase/commit/712c650a310d8db89f9ab94d9d820b26af00cf18))
* split metrics api into separate artifact ([#2631](https://www.github.com/googleapis/java-bigtable-hbase/issues/2631)) ([#3034](https://www.github.com/googleapis/java-bigtable-hbase/issues/3034)) ([42ec2ad](https://www.github.com/googleapis/java-bigtable-hbase/commit/42ec2ad3b7cbf427c15494a2f42a4000ac7ce91c))
* remove gcj wrappers from bigtable-client-core and remove BIGTABLE_USE_GCJ_CLIENT  ([#3057](https://www.github.com/googleapis/java-bigtable-hbase/issues/3057)) ([4ce242d](https://www.github.com/googleapis/java-bigtable-hbase/commit/4ce242da82a7330757e7e6a945dfaecd8c71ab2d))


### Bug Fixes

* Extend retry timeout for batch jobs from 5mins to 20 mins. ([#3050](https://www.github.com/googleapis/java-bigtable-hbase/issues/3050)) ([b5aad36](https://www.github.com/googleapis/java-bigtable-hbase/commit/b5aad366925fe0a0fbc55a8e92fafd03fc893451))
* use binary search in async region locator ([#3045](https://www.github.com/googleapis/java-bigtable-hbase/issues/3045)) ([857af34](https://www.github.com/googleapis/java-bigtable-hbase/commit/857af344e1c242ec3312f7ef725ffa5872446f24))


### Dependencies

* upgrade veneer to 1.27.1 ([#3054](https://www.github.com/googleapis/java-bigtable-hbase/issues/3054)) ([efed001](https://www.github.com/googleapis/java-bigtable-hbase/commit/efed0011ffba9552b83e69cd75323fde385eadb4))

## [1.22.0](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.21.1...v1.22.0) (2021-06-30)


### Features

* allow DirectPath by default + update integration tests ([#3031](https://www.github.com/googleapis/java-bigtable-hbase/issues/3031)) ([7c33b14](https://www.github.com/googleapis/java-bigtable-hbase/commit/7c33b14362614be0f2d3ad0b5cc95fa70f6a9add))

### [1.21.1](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.21.0...v1.21.1) (2021-06-29)


### Bug Fixes

* use binary search to get region location ([#3040](https://www.github.com/googleapis/java-bigtable-hbase/issues/3040)) ([7b8663f](https://www.github.com/googleapis/java-bigtable-hbase/commit/7b8663f31e805280ac594aaa98576c7d466eb1a2))

## [1.21.0](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.20.1...v1.21.0) (2021-06-22)


### Features

* add option to pass a tracing cookie ([#3014](https://www.github.com/googleapis/java-bigtable-hbase/issues/3014)) ([92ff6da](https://www.github.com/googleapis/java-bigtable-hbase/commit/92ff6daa0d9732af7fbee458b24166f22982a58b))


### Bug Fixes

* add tracing cookie to admin ([#3024](https://www.github.com/googleapis/java-bigtable-hbase/issues/3024)) ([bded191](https://www.github.com/googleapis/java-bigtable-hbase/commit/bded191bf0e12a8650a2616e49700dd194e25a62))
* keep only failed actions in List<Delete> ([#3007](https://www.github.com/googleapis/java-bigtable-hbase/issues/3007)) ([9b56c0c](https://www.github.com/googleapis/java-bigtable-hbase/commit/9b56c0cc071bb886d7021929efbd04781a7bab6f))
* max mutation comparison to include 100k ([#3009](https://www.github.com/googleapis/java-bigtable-hbase/issues/3009)) ([53acee3](https://www.github.com/googleapis/java-bigtable-hbase/commit/53acee346f5ce50b83fe02016e0bc53e0d69da09))


### Dependencies

* update shared config to 0.12.0 ([#3012](https://www.github.com/googleapis/java-bigtable-hbase/issues/3012)) ([b3c6b27](https://www.github.com/googleapis/java-bigtable-hbase/commit/b3c6b27d0d6a67884aded09eff74eef02db14df9))

### [1.20.1](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.20.0...v1.20.1) (2021-06-01)


### Bug Fixes

* fix buildSyncTableConfig naming ([#2968](https://www.github.com/googleapis/java-bigtable-hbase/issues/2968)) ([df30fb2](https://www.github.com/googleapis/java-bigtable-hbase/commit/df30fb210138ec2216c6e2034755dc32292317df))
* Refactoring methods in TemplateUtils. Using CamelCasing for names. ([#2967](https://www.github.com/googleapis/java-bigtable-hbase/issues/2967)) ([#2970](https://www.github.com/googleapis/java-bigtable-hbase/issues/2970)) ([cedd6d8](https://www.github.com/googleapis/java-bigtable-hbase/commit/cedd6d80aaf15ffa16f76978f3e14204b224de83))
* retry rst stream ([#3000](https://www.github.com/googleapis/java-bigtable-hbase/issues/3000)) ([914d65f](https://www.github.com/googleapis/java-bigtable-hbase/commit/914d65fb032d6069b7266fd439d29a8ad0c74777))

## [1.20.0](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.19.2...v1.20.0) (2021-05-11)


### Features

* Adding new dataflow pipelines to import HBase snapshot and import validation ([#2958](https://www.github.com/googleapis/java-bigtable-hbase/issues/2958)) ([d6f0604](https://www.github.com/googleapis/java-bigtable-hbase/commit/d6f06049b1c6a0653168d1c8814ac0367eb6a2ee))
* HBase to Cloud Bigtable schema translator.  ([#2954](https://www.github.com/googleapis/java-bigtable-hbase/issues/2954)) ([a8b0d83](https://www.github.com/googleapis/java-bigtable-hbase/commit/a8b0d837daa651fe9539c8f963d71a5c9338d7c4))

### [1.19.2](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.19.1...v1.19.2) (2021-04-20)


### Bug Fixes

* fix retry so it won't fail when rows read == rows limit ([#2925](https://www.github.com/googleapis/java-bigtable-hbase/issues/2925)) ([b6fb4c7](https://www.github.com/googleapis/java-bigtable-hbase/commit/b6fb4c70a0c4bd5b8d20efff408800592e147cf8))
* multithreaded batch operations ([#2932](https://www.github.com/googleapis/java-bigtable-hbase/issues/2932)) ([d6dc825](https://www.github.com/googleapis/java-bigtable-hbase/commit/d6dc825551a35e2623874a95f5812ca7863ee46d))

### [1.19.1](https://www.github.com/googleapis/java-bigtable-hbase/compare/v1.19.0...v1.19.1) (2021-03-18)


### Bug Fixes

* bigtable-hbase-2.x-hadoop incompatible with hbase-shaded-client 2.x ([#2773](https://www.github.com/googleapis/java-bigtable-hbase/issues/2773)) ([#2864](https://www.github.com/googleapis/java-bigtable-hbase/issues/2864)) ([b74ba4f](https://www.github.com/googleapis/java-bigtable-hbase/commit/b74ba4f5f4de0001391b85ffc9669ca46c187faa))
* Disabling Dataflow memory monitor for Bigtable Dataflow pipelines. ([#2856](https://www.github.com/googleapis/java-bigtable-hbase/issues/2856)) ([2af6202](https://www.github.com/googleapis/java-bigtable-hbase/commit/2af620239fa18a06eccb1547e92e82f15be71e47))
* hbase 2x shell ([#2905](https://www.github.com/googleapis/java-bigtable-hbase/issues/2905)) ([e74015b](https://www.github.com/googleapis/java-bigtable-hbase/commit/e74015b473d031edd77d896b05c55aa48c3848d4))
* updated assertions and scan for firstKeyOnlyFilter test ([#2483](https://www.github.com/googleapis/java-bigtable-hbase/issues/2483)) ([#2854](https://www.github.com/googleapis/java-bigtable-hbase/issues/2854)) ([c150262](https://www.github.com/googleapis/java-bigtable-hbase/commit/c150262267734164080c6ab5e3f991a140067408))


### Dependencies

* Update beam version to 2.24.0 ([#2857](https://www.github.com/googleapis/java-bigtable-hbase/issues/2857)) ([94aea7c](https://www.github.com/googleapis/java-bigtable-hbase/commit/94aea7c1e4260b067ab429f40ee018abfd3e22f7))
* update shared config to 0.10.0 ([#2826](https://www.github.com/googleapis/java-bigtable-hbase/issues/2826)) ([a20c746](https://www.github.com/googleapis/java-bigtable-hbase/commit/a20c7466412f1b97a4c00fc8611fe05b057d7b6d))
