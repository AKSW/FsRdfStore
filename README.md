# FsRdfStore
Transactional implementation of Jena's DatasetGraph backed by the file system

The code from [file backed dataset](https://github.com/SmartDataAnalytics/jena-sparql-api/tree/develop/jena-sparql-api-file-backed-dataset) should be refactored into this repository.


* Naming: FsRdfStore is not a nice name, maybe something along the lines of dataset in files system (difs), or fbd or fsbd... let's use difs for now.


* difs-core: The transactional DatasetGraph implementation with file system based indexing support via symbolic links
* difs-system: A system built on core that adds RDF-based configuration infrastructure.
* difs-cli: Command line tooling. E.g. start a fuseki with that dataset or reindex existing data.

## Differences to conventional triple stores

* One file for one or more named graphs
* File location depends on the graph URI and follows Java conventions.
* Files can be any RDF format (text or binary). The formor allows for use of firebird with version control systems such as GIT or SVN
  * In this case replication of a firebird store is a simple as a `git pull` or `svn update`.
* The files and folders can be exposed using WebDav allowing clients to perform queries to a remote firebird store
* Main use case is efficient and flexibile lookup of RDF metadata using identifiers and property values

## WebDAV Access

### Apache

This example assumes the folder to be exposed as webdav exists and is located at `/var/www/webdav`.

Because of legacy issues the Apache Virtual File System VFS2 cannot handle redirects on WebDav requests.
For this reason, the DirectorySlash must be turned off, such that `webdav://host/foo` does not get redirectod to `webdav://host/foo/`.

```
echo "DirectorySlash Off" > /var/www/webdav/.htaccess
sudo chown www-data:www-data /var/www/webdav/.htaccess
```


```
# /etc/apache2/site-available/000-default.conf

        DavLockDB /usr/local/apache2/var/DavLock
        Alias /webdav "/var/www/webdav"
        <Directory "/var/www/webdav">
                AllowOverride all
                DAV on
        </Directory>
```



## Storage Layout for Data, Locks, Transactions and Indexes

```
./store
./index
./locks
./txns
firebird.conf.ttl
```

* Transactions link to the accessed resources - such as by symlinking to the container in ./store
* Lock files in flatened container folders under ./locks link to the txns that own them

```
# Transactions link to the store folder - which allows obtaining the container name by relativizing the link target against the store base path
containerName = iriToPath(iri)
./store/foo/bar/data.trig
./txns/txn-123/.${lockRepo.getPath(containerName)} -> ../../store/foo/bar/data.trig

# Locks link back to the transaction that holds them
./locks/abcde/txn-123.read.lock -> ../txn/txn-123
```


