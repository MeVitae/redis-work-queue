# Integration Tests

## Running the tests

### Start a redis server

To start a redis server without persistence, you can run:

```bash
echo -e 'save ""\nappendonly no' | redis-server -
```

### Run the tests

From the `tests` directory, run:

```bash
./run-test.sh
```

## Unit tests also exist

Each client implementation contains some unit tests. These are located within the implementations
directory.

This directory contains the source for example workers, in each language, and a script to spawn jobs
and check the workers behave as expected.
