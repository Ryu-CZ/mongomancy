# Changelog

All notable changes to [mongomancy](https://github.com/Ryu-CZ/mongomancy) project will be
documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
