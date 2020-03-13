# Changelog

## [0.2.0](https://www.github.com/googleapis/java-bigtable-hbase/compare/0.1.4...v0.2.0) (2020-03-13)


### Features

* adding client wrapper interfaces ([#2406](https://www.github.com/googleapis/java-bigtable-hbase/issues/2406)) ([1ad48d9](https://www.github.com/googleapis/java-bigtable-hbase/commit/1ad48d9418e4d25180ca3dc66aaefcf80e00314b))
* enable directpath ([#2271](https://www.github.com/googleapis/java-bigtable-hbase/issues/2271)) ([d384208](https://www.github.com/googleapis/java-bigtable-hbase/commit/d384208c1d727fe99e57a10cc7ba8a5e4b75a041))
* extend RowCell to include labels field ([#2397](https://www.github.com/googleapis/java-bigtable-hbase/issues/2397)) ([80a85fe](https://www.github.com/googleapis/java-bigtable-hbase/commit/80a85fee02958d0fbe4fb32c6c809db7c0803358))


### Bug Fixes

* beam warning about accessing template values ([#2400](https://www.github.com/googleapis/java-bigtable-hbase/issues/2400)) ([443a40c](https://www.github.com/googleapis/java-bigtable-hbase/commit/443a40c65f40089a5cc41b5304a1422d1f8cd7ab)), closes [#2389](https://www.github.com/googleapis/java-bigtable-hbase/issues/2389)
* includes fixes for the gap between HBase and this client ([#2267](https://www.github.com/googleapis/java-bigtable-hbase/issues/2267)) ([51f0617](https://www.github.com/googleapis/java-bigtable-hbase/commit/51f0617aa8c9865b066e9bfaa25f053c7f0ad3d4))
* the options of CreateTableHelper to be public ([#2366](https://www.github.com/googleapis/java-bigtable-hbase/issues/2366)) ([072ccc9](https://www.github.com/googleapis/java-bigtable-hbase/commit/072ccc9a0bd16efb5c4dab5900447a3d2789bac8))
* update url in pom file ([#2279](https://www.github.com/googleapis/java-bigtable-hbase/issues/2279)) ([2e2f638](https://www.github.com/googleapis/java-bigtable-hbase/commit/2e2f638add2e4eb69e6b241c98f2f420527396ab))
* validation of TimeRange ([#1890](https://www.github.com/googleapis/java-bigtable-hbase/issues/1890)) ([7f404e4](https://www.github.com/googleapis/java-bigtable-hbase/commit/7f404e4f21004f8b552f4c5b80d409496f4a7240))


### Reverts

* Revert "[maven-release-plugin] prepare release bigtable-client-parent-1.13.0" ([7313467](https://www.github.com/googleapis/java-bigtable-hbase/commit/7313467fd6937331def4e54ed5a6c59592d37e17))
* Revert "[maven-release-plugin] prepare for next development iteration" ([e749835](https://www.github.com/googleapis/java-bigtable-hbase/commit/e749835440c819b97bcf3ffac91c0b1f24bf3476))
* Revert "Fix: includes fixes for the gap between HBase and this client (#2267)" (#2282) ([966e1b0](https://www.github.com/googleapis/java-bigtable-hbase/commit/966e1b0d0f8fc62a9fa6778bd3d34669402b52a5)), closes [#2267](https://www.github.com/googleapis/java-bigtable-hbase/issues/2267) [#2282](https://www.github.com/googleapis/java-bigtable-hbase/issues/2282)
* Revert "fix executors on app engine (#2111)" (#2112) ([89cc282](https://www.github.com/googleapis/java-bigtable-hbase/commit/89cc2829b30498f332095a5f2211e6fa57f95b4a)), closes [#2111](https://www.github.com/googleapis/java-bigtable-hbase/issues/2111) [#2112](https://www.github.com/googleapis/java-bigtable-hbase/issues/2112)
* Revert "fix lint for test (#1978)" (#1979) ([fcf32f4](https://www.github.com/googleapis/java-bigtable-hbase/commit/fcf32f4befc00e6b9545cbc9457350f93ad36821)), closes [#1978](https://www.github.com/googleapis/java-bigtable-hbase/issues/1978) [#1979](https://www.github.com/googleapis/java-bigtable-hbase/issues/1979)
* Revert "Enable throttling in BufferedMutator by default (#1854)" (#1856) ([4eaa615](https://www.github.com/googleapis/java-bigtable-hbase/commit/4eaa6151c4bafe389be343278374765096146f1d)), closes [#1854](https://www.github.com/googleapis/java-bigtable-hbase/issues/1854) [#1856](https://www.github.com/googleapis/java-bigtable-hbase/issues/1856)
* Revert "Convert parameters in CloudBigtableConfiguration to runtime parameters (#1813)" (#1814) ([3aa75e9](https://www.github.com/googleapis/java-bigtable-hbase/commit/3aa75e922fcc7aa327b995763129bb522799e4d4)), closes [#1813](https://www.github.com/googleapis/java-bigtable-hbase/issues/1813) [#1814](https://www.github.com/googleapis/java-bigtable-hbase/issues/1814)
* Revert "Adding a Beam version of CloudBigtableIO." ([0cc334a](https://www.github.com/googleapis/java-bigtable-hbase/commit/0cc334a076fa2d1ecc2d2f376b7d2070827b49f1))


### Dependencies

* add maven-enforcer-plugin ([#2401](https://www.github.com/googleapis/java-bigtable-hbase/issues/2401)) ([1ef4b9c](https://www.github.com/googleapis/java-bigtable-hbase/commit/1ef4b9cb23a11d630114f382080baac66fa2f26d))
* fix dependency build ([#2305](https://www.github.com/googleapis/java-bigtable-hbase/issues/2305)) ([e92fd2e](https://www.github.com/googleapis/java-bigtable-hbase/commit/e92fd2e560421059bda83a0c0fd2f11f73ba9fcd))
* fix hbaseLocalMiniClusterTestH2 tests ([#2308](https://www.github.com/googleapis/java-bigtable-hbase/issues/2308)) ([b7f48f5](https://www.github.com/googleapis/java-bigtable-hbase/commit/b7f48f5d7844af2e5466c8be3437edd7fe05e152))
* manually update dependency org.mockito:mockito-core to v3.3.0 ([#2392](https://www.github.com/googleapis/java-bigtable-hbase/issues/2392)) ([918340d](https://www.github.com/googleapis/java-bigtable-hbase/commit/918340dfe3abf60411624fee6b6aa826644e24d9))
* update bigtable.version to v1.11.0 ([#2395](https://www.github.com/googleapis/java-bigtable-hbase/issues/2395)) ([50de5d7](https://www.github.com/googleapis/java-bigtable-hbase/commit/50de5d76520485ec8e13a0b481a4b88dcd3fd56c))
* update dependency com.fasterxml.jackson.core:jackson-databind to v2.10.2 ([#2340](https://www.github.com/googleapis/java-bigtable-hbase/issues/2340)) ([437557f](https://www.github.com/googleapis/java-bigtable-hbase/commit/437557fd108db51273dd97d9d0eb1c9bfeecda6d))
* update dependency com.fasterxml.jackson.core:jackson-databind to v2.10.3 ([#2394](https://www.github.com/googleapis/java-bigtable-hbase/issues/2394)) ([95834d1](https://www.github.com/googleapis/java-bigtable-hbase/commit/95834d1623546c028c347ede6fe1f0169a66de00))
* update dependency commons-codec:commons-codec to v1.14 ([#2339](https://www.github.com/googleapis/java-bigtable-hbase/issues/2339)) ([41a3d98](https://www.github.com/googleapis/java-bigtable-hbase/commit/41a3d98cb4561917ac91e1ea9207b99e1f3e0067))
* update dependency io.grpc:grpc-bom to v1.26.0 ([6c616c9](https://www.github.com/googleapis/java-bigtable-hbase/commit/6c616c90d1e4c280b62a74e37bd84cdb6b7ec76e))
* update dependency junit:junit to v4.13 ([#2335](https://www.github.com/googleapis/java-bigtable-hbase/issues/2335)) ([92537f0](https://www.github.com/googleapis/java-bigtable-hbase/commit/92537f088691ea10de87ef6a580ae8f22c323132))
* update dependency org.apache.commons:commons-compress to v1.20 ([#2376](https://www.github.com/googleapis/java-bigtable-hbase/issues/2376)) ([65ac9cd](https://www.github.com/googleapis/java-bigtable-hbase/commit/65ac9cdfd7a45fcf663dd49f79896494ea993ea2))
* update dependency org.mockito:mockito-core to v3.2.4 ([#2290](https://www.github.com/googleapis/java-bigtable-hbase/issues/2290)) ([2e2e4f9](https://www.github.com/googleapis/java-bigtable-hbase/commit/2e2e4f9d6df349232e16f4bebe184de973edf8d2))
* update dependency org.slf4j:slf4j-api to v1.7.30 ([#2309](https://www.github.com/googleapis/java-bigtable-hbase/issues/2309)) ([8182b5d](https://www.github.com/googleapis/java-bigtable-hbase/commit/8182b5d3e492713738ef2f8589ef714c2cabfa5b))
* update jmh.version to v1.22 ([#2348](https://www.github.com/googleapis/java-bigtable-hbase/issues/2348)) ([cdec57f](https://www.github.com/googleapis/java-bigtable-hbase/commit/cdec57fc7b10bbea62c8f7def6ada389849aa970))
* update jmh.version to v1.23 ([#2370](https://www.github.com/googleapis/java-bigtable-hbase/issues/2370)) ([4959c8f](https://www.github.com/googleapis/java-bigtable-hbase/commit/4959c8f0ca119e19a7585875ba056934dda60957))


### Documentation

* add javadoc for classes marked as InternalApi ([#2350](https://www.github.com/googleapis/java-bigtable-hbase/issues/2350)) ([c86d7d2](https://www.github.com/googleapis/java-bigtable-hbase/commit/c86d7d265b2816136e3d6313eeb8fa87d74f9635))
* Updated readme to redirect user to veneer client ([#2288](https://www.github.com/googleapis/java-bigtable-hbase/issues/2288)) ([aa9ac47](https://www.github.com/googleapis/java-bigtable-hbase/commit/aa9ac475ca6633dd38d338192ca9ddae7a423188))
