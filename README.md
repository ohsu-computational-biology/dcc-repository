ICGC DCC - Repository
===

Importer for the ICGC "Data Repository" feature which imports file metadata from various external data sources.

Modules
---

The Repository system is comprised of the following modules.

### Core

This module is the shared understanding of the system
- [Core](dcc-repository-core/README.md)

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

Build
---

From the command line:

	mvn clean package
	
Changes
---

See [CHANGES.md](CHANGES.md)

