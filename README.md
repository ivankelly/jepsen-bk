# jepsen.bk

A Jepsen test for bookkeeper. It starts a bookie, zookeeper node, and a simple register service and runs a simple jepsens test with the partition nemesis against it. The register service is a simple rest service that elects a leader (using zookeeper) and maintains a log of all the updates written to a single register. The bookeeper ledger changes each time the leader changes. The list of ledger is stored in bookkeeper.

Failure in this test could mean a bug in the register service or a bug in bookkeeper. Probably not a bug in zookeeper as it has already passed a battery of jepsen tests.

## Usage

This has only been tested on a node running Debian 'Stretch'.

1. Install leiningen [1].

2. Either set up a bunch of lxc nodes
```
$ bash cluster.sh build
```
or create 5 debian stretch instances in AWS or whatnot and put their IPs in a file. Look at cluster.sh to see what extra setup is needed on the nodes.

3. Run the test, pointing to the file with the IPs. cluster.sh creates a file call nodes.latest.
```
$ lein run test --nodes-file nodes.latest
```

4. Run the jepsen http server to explore the results.
```
$ lein run serve
```

[1] https://leiningen.org/

## License

Copyright © 2017 Ivan Kelly <ivan@ivankelly.net>

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
