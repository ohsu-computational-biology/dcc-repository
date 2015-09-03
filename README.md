ICGC DCC - Repository
===

Importer for the ICGC "Data Repository" feature which imports file metadata from various external data sources.

Modules
---

Sub-system modules:

- [Core](dcc-repository-core/README.md)
- [AWS](dcc-repository-aws/README.md)
- [CGHub](dcc-repository-cghub/README.md)
- [PCAWG](dcc-repository-pcawg/README.md)
- [TCGA](dcc-repository-tcga/README.md)

Build
---

From the command line:

	mvn -pl dcc-repository-client -am clean package -DskipTests
	
Changes
---

See [CHANGES.md](CHANGES.md)

