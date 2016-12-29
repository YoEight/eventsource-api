# Haskell Eventsourcing

Home of an attempt to formalize eventsourcing.

We use [stack][] to build project under this repository.

## How to work on the entire stack ?

```sh
# Build the code
$ stack --stack-yaml=stack-all.yaml build

# Test the code
$ stack --stack-yaml=stack-all.yaml test
```

## About testing

Some packages may need prior configuration in order to run their tests (like a live database server)

[stack]: http://www.haskellstack.com
