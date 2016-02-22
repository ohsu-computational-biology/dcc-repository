# ICGC DCC - Repository - Client

Client module that is the execution entry point into the system.

## Build

To compile, test and package the module, execute the following from the root of the repository:

```shell
mvn -am -pl dcc-repository/dcc-repository-client
```

## Configuration

Configuration template may be found [here](src/main/conf).

## Command Line Interface

```
Usage: dcc-repository-client [options]
  Options:
  *     --config
       Path to the repository config file
        --sources
       Source to import. Comma seperated list of: 'aws', 'pcawg', 'tcga',
       'cghub'. By default all sources will be imported.
       Default: [CGHUB, TCGA, PCAWG, AWS]
```
