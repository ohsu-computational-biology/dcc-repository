# ICGC DCC - Repository - Excloud

Excloud import module. Indexes donor, sample and files from BAML exacloud


## Build

To compile, test and package the module, execute the following from the root of the repository:

```shell
mvn -am -pl dcc-repository/dcc-repository-excloud
```

## Execution

### Navigate to
`ubuntu@dcc-etl-2:~/icgc-dcc/dcc-repository`

### Run
`mvn install`

### Navigate to
`ubuntu@dcc-etl-2:~/icgc-dcc/dcc-repository/dcc-repository-client/src/main/bin`

### Config

Note, in bin there is a python `install` command.  I have been unable to get it to work.
The following steps recreate that command manually
* Create the following directories
   ..\conf
   ..\lib

* In ..\conf
   cp ../resources/application.yml .
   edit application.yml
   ```
   esUri  -- set according to your environment
   mongoUri -- set according to your environment
# ID
id:
  #serviceUrl: https://localhost:8443 -- replace with empty string, this forces dcc-repository to use hash identity service
  serviceUrl:
   ```
* specify location of extract fgorm exacloud (produced by https://github.com/ohsu-computational-biology/ccc_data/blob/master/BAML/lls-scor/exacloud/aml_paths.sh )
export EXACLOUD_ARCHIVE_LISTING="file:///mnt/etl/dcc-release-mayfielg/release/BAML-US/lls_resource.tsv"

### Run

From `ubuntu@dcc-etl-2:~/icgc-dcc/dcc-repository/dcc-repository-client/src/main/bin`
$ ./dcc-repository-client.sh

Note: you will see missing credentials messages for some repositories
* AWS credential profiles file not found in the given path

Eventually you will see a message similar to:
Assigning index alias test-icgc-repository to index test-icgc-repository-20161007

### Test
use kibana to explore the new index

### Use

curl -XPOST 'http://10.60.60.48:9200/_aliases' -d '
{
    "actions" : [
        { "add" : { "index" : "test-icgc-repository-20161007", "alias" : "icgc-repository" } }
    ]
}'

Now the new index should appear in the portal.

