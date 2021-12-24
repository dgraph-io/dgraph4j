# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.1.0/),
and this project adheres to [Calendar Versioning](https://calver.org/) starting v20.03.0.

## [Unreleased]

## [21.12.0] - 2021-12-24

* Version bump. No changes from v21.03.2.

## [21.03.2] - 2021-12-24

### Added

* feat: Add timeout parameters to transaction requests. ([#172])
* feat: Add shutdown method to client. ([#173])

[#172]: https://github.com/dgraph-io/dgraph4j/pull/172
[#173]: https://github.com/dgraph-io/dgraph4j/pull/173

## [21.03.1] - 2021-04-22
### Added
* fix(deprecation): add DgraphClient.clientStubFromCloudEndpoint ([#170])

[#170]: https://github.com/dgraph-io/dgraph4j/pull/170

## [21.03.0] - 2021-04-08
### Added
* feat: add loginIntoNamespace API on clients (GRAPHQL-1058) ([#166])
### Deprecated
* chore: Sync dgo + deprecate Slash endpoint method (GRAPHQL-1141) ([#167])
### Fixed
* fix(test): Don't use dgraph binary in tests (GRAPHQL-1144) ([#168])

[#166]: https://github.com/dgraph-io/dgraph4j/pull/166
[#167]: https://github.com/dgraph-io/dgraph4j/pull/167
[#168]: https://github.com/dgraph-io/dgraph4j/pull/168

## [20.11.0] - 2020-12-23
### Added
* feat: Add support for RDF response (DGRAPH-2659) ([#160])
### Changed
* gRPC: bumped gRPC libs to v1.34.1

[#160]: https://github.com/dgraph-io/dgraph4j/pull/160

## [20.03.3] - 2020-11-25
### Added
* feat: add client constructor with executor (DGRAPH-2746) ([#161])

[#161]: https://github.com/dgraph-io/dgraph4j/pull/161

## [20.03.2] - 2020-10-27
### Added
* feat: Support for Slash GraphQL endpoint ([#158])

[#158]: https://github.com/dgraph-io/dgraph4j/pull/158

## [20.03.1] - 2020-07-10
### Added
* Allow creation of Transaction/AsyncTransaction from TxnContext ([#149])
* feat: Client now has a checkVersion method ([#155])
### Fixed
* Delete argument from method usePlainText ([#148])

[#149]: https://github.com/dgraph-io/dgraph4j/pull/149
[#155]: https://github.com/dgraph-io/dgraph4j/pull/155
[#148]: https://github.com/dgraph-io/dgraph4j/pull/148

## [20.03.0] - 2020-04-01
### Changed
* Upgrade grpc version to 1.26.0 ([#130])
* Add docs for background indexing, update api.proto ([1324663])
### Fixed
* Fix slf4j dependencies. ([#133])
* Fixed running multiple upserts within one transaction ([#137])

[#130]: https://github.com/dgraph-io/dgraph4j/pull/130
[1324663]: https://github.com/dgraph-io/dgraph4j/commit/13246633dc87ab884beadf2ac239116890055b48
[#133]: https://github.com/dgraph-io/dgraph4j/pull/133
[#137]: https://github.com/dgraph-io/dgraph4j/pull/137

## [2.1.0] - 2020-01-30
### Changed
* Sync proto files with dgo ([#121])

[#121]: https://github.com/dgraph-io/dgraph4j/pull/121

## [2.0.2] - 2019-10-13
### Fixed
* Fix Opencensus tracing. ([#117])

[#117]: https://github.com/dgraph-io/dgraph4j/pull/117

## [2.0.1] - 2019-09-05
### Fixed
* Throw TxnConflictException correctly ([#102])

[#102]: https://github.com/dgraph-io/dgraph4j/pull/102

## [2.0.0] - 2019-09-02
### Changed
* Upgrade grpc to get rid of Java 11 warnings from grpc ([#94])
* Update grpc API to support Multiple Mutations in future ([#101])

[#94]: https://github.com/dgraph-io/dgraph4j/pull/94
[#101]: https://github.com/dgraph-io/dgraph4j/pull/101

## [1.7.5] - 2019-06-29
### Added
* Add support for upsert block ([#88])
### Fixed
* Moving the dependency io.opencensus.* into a test dependency only, ([#89])

[#88]: https://github.com/dgraph-io/dgraph4j/pull/88
[#89]: https://github.com/dgraph-io/dgraph4j/pull/89

## [1.7.4] - 2019-06-19
### Added
* Added best effort method to the transaction class to call the AsyncTransaction class method. ([#86])
### Fixed
* Fix broken tests ([#87])

[#86]: https://github.com/dgraph-io/dgraph4j/pull/86
[#87]: https://github.com/dgraph-io/dgraph4j/pull/87

## [1.7.3] - 2019-03-27
### Added
* added logic for ACL and upgraded the code to be usable with java 11 ([#80])
* Added best effort flag, shorten ACL test timing to match changes in dgraph ([b4bf3bf])

[#80]: https://github.com/dgraph-io/dgraph4j/pull/80
[b4bf3bf]: https://github.com/dgraph-io/dgraph4j/commit/b4bf3bf49d7f89f0bffd3a4929f42efbef9a5a31

## [1.7.1] - 2018-10-31
### Added
* Added the doc and sample code for setting auth token ([#70])
* Readonly support, separating integration tests and unit tests etc ([#71])
### Fixed
* Fix client selection behavior to match Go client. ([3bb5e23])

[#70]: https://github.com/dgraph-io/dgraph4j/pull/70
[#71]: https://github.com/dgraph-io/dgraph4j/pull/71
[3bb5e23]: https://github.com/dgraph-io/dgraph4j/commit/3bb5e23c6bcbfd7224a602aa34ef33c4c26c24f4

## [1.7.0] - 2018-09-20
* Upgrading grpc-java to v1.15.0

## [1.6.0] - 2018-09-11
* Remove LinRead and sequencing map from client.

## [1.5.0] - 2018-09-03
* Updated api.proto. Added a new preds field to TxnContext.

## [1.4.2] - 2018-08-21
* Added Async client.
* Fixed maven publishing issues

## [1.3.0] - 2018-04-04
* Added option to choose Client/Serve Side sequential mode.

## [1.2.0] - 2018-02-06
* Added possibility to create a DgraphClient with a specified request
  deadline (#48).

## [1.1.0] - 2018-01-17
* Incorporated proto file changes in Dgraph 1.2.0 release

## [1.0.0] - 2017-12-18
* Fully compatible with Dgraph v1.0

[Unreleased]: https://github.com/dgraph-io/dgraph4j/compare/v21.12.0...HEAD
[21.12.0]: https://github.com/dgraph-io/dgraph4j/compare/v21.03.2...v21.12.0
[21.03.2]: https://github.com/dgraph-io/dgraph4j/compare/v21.03.1...v21.03.2
[21.03.1]: https://github.com/dgraph-io/dgraph4j/compare/v21.03.0...v21.03.1
[21.03.0]: https://github.com/dgraph-io/dgraph4j/compare/v20.11.0...v21.03.0
[20.11.0]: https://github.com/dgraph-io/dgraph4j/compare/v20.03.3...v20.11.0
[20.03.3]: https://github.com/dgraph-io/dgraph4j/compare/v20.03.2...v20.03.3
[20.03.2]: https://github.com/dgraph-io/dgraph4j/compare/v20.03.1...v20.03.2
[20.03.1]: https://github.com/dgraph-io/dgraph4j/compare/v20.03.0...v20.03.1
[20.03.0]: https://github.com/dgraph-io/dgraph4j/compare/v2.1.0...v20.03.0
[2.1.0]: https://github.com/dgraph-io/dgraph4j/compare/v2.0.2...v2.1.0
[2.0.2]: https://github.com/dgraph-io/dgraph4j/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/dgraph-io/dgraph4j/compare/v1.7.4...v2.0.1
[1.7.4]: https://github.com/dgraph-io/dgraph4j/compare/v1.7.3...v1.7.4
[1.7.3]: https://github.com/dgraph-io/dgraph4j/compare/v1.7.0...v1.7.3
[1.7.0]: https://github.com/dgraph-io/dgraph4j/compare/v1.6.0...v1.7.0
[1.7.0]: https://github.com/dgraph-io/dgraph4j/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/dgraph-io/dgraph4j/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/dgraph-io/dgraph4j/compare/v1.4.2...v1.5.0
[1.4.2]: https://github.com/dgraph-io/dgraph4j/compare/v1.3.0...v1.4.2
[1.3.0]: https://github.com/dgraph-io/dgraph4j/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/dgraph-io/dgraph4j/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/dgraph-io/dgraph4j/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/dgraph-io/dgraph4j/tree/v1.0.0
