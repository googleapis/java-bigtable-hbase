# Changelog

## [1.20.0-sp.2](https://www.github.com/googleapis/java-bigtable-hbase/compare/1.20.0-sp.1...v1.20.0-sp.2) (2021-08-04)


### Dependencies

* upgrade commons-compress to 1.21 ([#3173](https://www.github.com/googleapis/java-bigtable-hbase/issues/3173)) ([2d811e9](https://www.github.com/googleapis/java-bigtable-hbase/commit/2d811e98b52f116ad9474b67a404e15f7022c0e3))

## [1.20.0-sp.1](https://www.github.com/googleapis/java-bigtable-hbase/compare/1.20.0...v1.20.0-sp.1) (2021-05-25)


### Dependencies

* update bigtable.version to 1.22.0-sp.1 and shared config to 0.11.0 ([#2989](https://www.github.com/googleapis/java-bigtable-hbase/issues/2989)) ([c457c62](https://www.github.com/googleapis/java-bigtable-hbase/commit/c457c62757d836b56fad5d0f60ede76220800b5e))

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
