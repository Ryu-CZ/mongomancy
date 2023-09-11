# Changelog

All notable changes to [mongomancy](https://github.com/Ryu-CZ/mongomancy) project will be
documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]


## [0.1.14] - 2023-09-11

### Fixed

- missing attribute fixed for `AttributeError: 'Collection' object has no attribute 'database'`, now covered by tests

## [0.1.13] - 2023-06-15

### Changed

- reduced default `max_wait` of database to 55 sec

### Fixed

- perform db unlock on fail

## [0.1.12] - 2023-06-06

### Added

- unittests

### Changed

- typing and small fixes on reconnect
- separate base requirements and requirements-ev

## [0.1.11] - 2023-05-15

### Changed

- you can now supply mongo client class to `Engine` constructor for example the `mongomock` with `mongo_client_cls=mongomock.MongoClient`

## [0.1.10] - 2023-05-12

### Fix

- ping now work even if connection is broken, and does nto fall into infinite loop

## [0.1.7] - 2023-05-10

### Fix

- lock critical sections `Engine.reconnect`, `Engine.dispose` to prevent unexpected connections states
- lock `Database.create_all` method to prevent double init at same

## [0.1.6] - 2023-05-10

### Fix

- broken default data insert from last commit

## [0.1.5] - 2023-05-10

### Fixed

- reset references correctly after dispose
- query repeat on fail - queries are now actually repeated on new collection instance instead of disconnected one

## [0.1.4] - 2023-05-09

### Fixed

- tracing if engine dispose to prevent closed connection error

## [0.1.3] - 2023-05-09

### Fixed

- ignore double init on collection

## [0.1.2] - 2023-05-09

### Fixed

- unlock called bad collection

## [0.1.1] - 2023-05-09

### Added

- introduced lock wait timeout into `Database` init

### Fixed

- unlock database after first init

## [0.1.0] - 2023-05-09

### Added

- mongomancy creates `mongomancy_lock` collection to synchronize db init with master lock record
- create all now uses multiprocess and thread log

## [0.0.6] - 2023-04-13

### Fixed

- typing of required fields of index more broad

## [0.0.5] - 2023-04-12

### Added

- Include `py.typed` for python typing support.

## [0.0.4] - 2023-04-04

### Fixed

- Collection aggregate return type set to pymongo CommandCursor.

## [0.0.3] - 2023-04-04

### Added

- Collection and Engine now support `aggregate` method.

### Fixed

- Hooks typing now correctly allows any Executor subclass.
- Index fields accepts dict type as valid ordered dict in python 3.7+ because it is guaranteed to keep order.

## [0.0.2] - 2022-08-29

### Changed

- tune build process

## [0.0.1] - 2022-08-29

### Added

- First stable version.

### Fixed

- Auto-reconnect to database on remote cluster master switch.

## [0.0.0] - 2022-08-26

### Added

- Begin of changelog.
