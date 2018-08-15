# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]
* Use a separate `DgraphClientPool` class to manage GRPC clients.

## [1.3.0] - 2018-04-04
* Added option to choose Client/Serve Side sequential mode.

## [1.2.0] - 2018-02-06
* Added possibility to create a DgraphClient with a specified request
  deadline (#48).

## [1.1.0] - 2018-01-17
* Incorporated proto file changes in Dgraph 1.2.0 release

## [1.0.0] - 2017-12-18
* Fully compatible with Dgraph v1.0

[Unreleased]: https://github.com/dgraph-io/dgraph4j/compare/v1.3.0...HEAD
[1.3.0]: https://github.com/dgraph-io/dgraph4j/compare/v1.1.0...v1.3.0
[1.2.0]: https://github.com/dgraph-io/dgraph4j/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/dgraph-io/dgraph4j/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/dgraph-io/dgraph4j/tree/v1.0.0
