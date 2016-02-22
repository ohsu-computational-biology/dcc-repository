# ICGC DCC - Repository

Importer for the ICGC "Data Repository" feature which imports file metadata from various external data sources.

## Build

To compile, test and package the system, execute the following from the root of the repository:

```shell
mvn
```

## Modules

The Repository system is comprised of the following modules.

### Common

This module is the shared understanding of the system
- [Core](dcc-repository-core/README.md)
- [Resources](dcc-repository-resources/README.md)

### Client

This is the main entry point of the application.
- [Client](dcc-repository-client/README.md)

### Sources

These modules collect various data sources.
- [AWS](dcc-repository-aws/README.md)
- [Collab](dcc-repository-collab/README.md)
- [CGHub](dcc-repository-cghub/README.md)
- [PCAWG](dcc-repository-pcawg/README.md)
- [TCGA](dcc-repository-tcga/README.md)

AWS and Collab share common code through:
- [Cloud](dcc-repository-cloud/README.md)

### Index

This module indexes the collected data sources.

- [Index](dcc-repository-index/README.md)
	
## Installation

For automated deployment and installation of the infrastructure and software components, please consult the [dcc-cm](https://github.com/icgc-dcc/dcc-cm/blob/develop/ansible/README.md) project.

## Changes

Change log for the user-facing system modules may be found in [CHANGES.md](CHANGES.md).

## License

Copyright and license information may be found in [LICENSE.md](LICENSE.md).
