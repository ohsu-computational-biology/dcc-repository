ICGC DCC - Repository - AWS
===

This is ICGC AWS S3 repository file import module. 

It's responsible for indexing the state of the S3 bucket managed by [dcc-storage-server](/../../dcc-storage/README.md). This includes the `id` of the file, its size and last modified date. The state and related metadata is derived from [ICGC-TCGA-PanCancer](https://github.com/ICGC-TCGA-PanCancer/s3-transfer-operations).
