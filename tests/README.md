# Integration Tests

## Running the tests

### Start a redis server

To start a redis server without persistence, you can run:

```bash
echo -e 'save ""\nappendonly no' | redis-server -
```

### The test script

From the `tests` directory, to run the integration tests with all languages, use:

```bash
./run-test.sh -t go_jobs,python_jobs,rust_jobs,node_jobs,dotnet_jobs
```

For a summary of other options, run:

```bash
./run-test.sh --help
```

#### Options

##### -- tests

You can use the --tests command to test specific implementations.

For example:

```bash
./run-test.sh --tests "go_jobs,python_jobs"
```

##### --host

This can be used to set a specific redis server, the default is `localhost`.

For example:

```bash
./run-test.sh --tests "go_jobs,python_jobs" --host example.server.net:port
```

## Unit tests also exist

Each client implementation contains some unit tests. These are located within the implementations
directory.

This directory contains the source for example workers, in each language, and a script to spawn jobs
and check the workers behave as expected.

