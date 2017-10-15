# `importar`

An event-based API for connecting producers of imported external data with consumers that consume and synchronise against such data.

## Overview

* Producers can start whenever they like, sending the `import_started` signal to allow consumers to consume the incoming data.
* Consumers' signal handlers get an `ImportOperation` object, allowing them to determine the type of import, and attach an `ImportOperationHandler` to receive incoming data if they are interested.
* The producers then push through their data, with any interested consumers handling it.

See [integration_test.py][integration-test] for a simple, complete example.

[integration-test]: tests/integration_test.py

## Caveat emptor

**This is very much an alpha-level proptype.** Test coverage is such that it most likely does what it says on the tin, but what the tin says may not be a good idea.
