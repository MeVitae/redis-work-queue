# DotNet Implementation

- Manual: [RedisWorkQueue.pdf](RedisWorkQueue.pdf)
- Implementation with README: [RedisWorkQueue](RedisWorkQueue)

## Building the docs

```bash
doxygen
make -C Doxygen-docs/latex
cp Doxygen-docs/latex/refman.pdf RedisWorkQueue.pdf
```
