### Very simple benchmarking tool for Redis

This tool is intended to be similar to `redis-benchmark` with a few additional capabilities:

* Able to specify multiple hosts in order for client connections to be round-robined

### Usage

    Usage of ./rbm:
     -c int
           number of clients to use (default 50)
     -churn
           delete entries immediately after creation
     -h string
           comma-separated host:port list (default "localhost:6379")
     -help
           help
     -i int
           iterations of the test to run - divided among clients (default 100000)
     -ignore-errors
           ignore errors from Redis calls
     -t string
           benchmark to run: sadd, smembers, del, pubsub (default "sadd")
     -x int
           variant 1 - test dependent.
             sadd: the range of sets to use
             smembers: the range of sets to use
             del: the range of sets to use
             pubsub: the number of subscribers and -c should be used for publishers (default 1)
     -y int
           variant 2 - test dependent.
             smembers: the number of elements to add to each set
             del: the number of entries to create in a set before deleting it (default 1)
            
### Commands supported

#### `sadd` benchmark test

`sadd` Uses both variants to adjust the test characteristics

- `-x int` Randomized value to select a given set to use for each operation
- `-y int` Randomized value to define the member to add to the set

#### `smembers` benchmark test

`smembers` pre-populates sets with members and then calls smembers on a random set for each iteration:

- `-x int` Randomized value to select a given set to use for each operation
- `-y int` Number of values to create in each set

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

