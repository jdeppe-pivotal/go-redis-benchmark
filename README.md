### Very simple benchmarking tool for Redis

This tool is intended to be similar to `redis-benchmark` with a few additional capabilities:

* Able to specify multiple hosts in order for client connections to be round-robined

### Usage
    Usage of ./rbm:
      -bulk
            sadd and srem will be given multiple members to add/remove based on -y option
      -c int
            number of clients to use (default 50)
      -churn
            delete entries immediately after creation by sadd benchmark
      -flush
            flush after each benchmark runs (default true)
      -h string
            comma-separated host:port list (default "localhost:6379")
      -help
            help
      -i int
            iterations of the test to run - divided among clients (default 100000)
      -ignore-errors
            ignore errors from Redis calls
      -t string
            benchmark to run: sadd, smembers, srem, del, pubsub, setOperations (default "setOperations")
      -x int
            variant 1 - test dependent.
              sadd: the range of sets to use
              srem: the range of sets to use
              smembers: the range of sets to use
              del: the range of sets to use
              pubsub: the number of subscribers and -c should be used for publishers (default 1)
      -y int
            variant 2 - test dependent.
              sadd: the range of random member names to add
              srem: the number of elements to add to each set
              smembers: the number of elements to add to each set
              del: the number of entries to create in a set before deleting it (default 1)``

### Commands supported

#### `hset` benchmark test

`hset` Uses both variants to adjust the test characteristics

- `-x int` Randomized value to select a given hash to use for each operation
- `-y int` Randomized value to define both the field and value to add to the hash

Typically you would use large `y` values so that a new entry is added for each iteration.

#### `sadd` benchmark test

`sadd` Uses both variants to adjust the test characteristics

- `-x int` Randomized value to select a given set to use for each operation
- `-y int` Randomized value to define the member to add to the set

Typically you would use large `y` values so that a new member is added for each iteration.

#### `smembers` benchmark test

`smembers` pre-populates sets with members and then calls smembers on a random set for each iteration:

- `-x int` Randomized value to select a given set to use for each operation
- `-y int` Number of values to create in each set

#### `srem` benchmark test

`srem` pre-populates sets with members, calls srem on one member of a random set, and then adds the 
removed member back to the set using `sadd` for each iteration:

- `-x int` Randomized value to select a given set to use for each operation
- `-y int` Number of values to create in each set

#### `setOperations` benchmark test

`setOperations` performs a random mix of the 4 set benchmark tests (`sadd`, `srem`, `smembers`, `del`)

#### `del` benchmark test

`del` Uses both variants to adjust the test characteristics

- `-x int` Randomized value to select a given set to use for each operation
- `-y int` Number of members to add to each set before deleting (for every iteration)

### Building

You will need go 1.13 to build. On Mac OS the easiest way to get go is simply to `brew install golang`

Build the utility with:

    $ go build

To cross-compile for another platform (example build a Linux exe on Mac)

    $ GOOS=linux GOARCH=amd64 go build

### Testing

[Ginkgo](https://github.com/onsi/ginkgo) is used for the few tests that do exist.
