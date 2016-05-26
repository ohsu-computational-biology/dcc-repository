# ICGC DCC - Repository - PDC

This is ICGC PCAWG [Bionimbus PDC](https://bionimbus-pdc.opensciencedatacloud.org/) repository file import module.

It's responsible for indexing the state of the S3 bucket managed by UoC. This includes the `id` of the file, its size and last modified date. The state and related metadata is derived from PCAWG.

## Build

To compile, test and package the module, execute the following from the root of the repository:

```shell
mvn -am -pl dcc-repository/dcc-repository-pdc
```
