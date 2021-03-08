# FsRdfStore
Transactional implementation of Jena's DatasetGraph backed by the file system

The code from [file backed dataset](https://github.com/SmartDataAnalytics/jena-sparql-api/tree/develop/jena-sparql-api-file-backed-dataset) should be refactored into this repository.


* Naming: FsRdfStore is not a nice name, maybe something along the lines of dataset in files system (difs), or fbd or fsbd... let's use difs for now.


* difs-core: The transactional DatasetGraph implementation with file system based indexing support via symbolic links
* difs-system: A system built on core that adds RDF-based configuration infrastructure.
* difs-cli: Command line tooling. E.g. start a fuseki with that dataset or reindex existing data.

