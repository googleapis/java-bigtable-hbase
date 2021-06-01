# Changelog

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
