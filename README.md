
# We changed home!

**The project is now hosted on https://gitlab.com/YoEight/eventsource-api-hs**

**This repository is left as-is for backward-compability. We will no longer take issue or PR here**

# Haskell Eventsourcing

Home of an attempt to formalize eventsourcing.

We use [stack][] to build project under this repository.

## How to work on the entire stack ?

```sh
# Build the code
$ stack build

# Test the code
# (GetEventStore store implementation will require a running GetEventStore server on 1113 port)
$ stack test
```

## About testing

Some packages may need prior configuration in order to run their tests (like a live database server)

[stack]: http://www.haskellstack.com
