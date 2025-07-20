# arc

ARC is a Python based backup and restore tool that stores blobs identified by the hash of their
contents to minimize duplicated storage.

An index of the stored files is kept in a SQLITE3 database and should be shipped together with the
storage blobs.  Files larger than 32MB will be split into 32MB chunks, each of which will be stored
separately in the archive backend.

The objectstore backends are provided by the 'multicloud' library

Configuration:

   The configuration for the archive is stored in 'config.yaml' with the following properties:

   archive:
    <archive-name>:
        verifyreads: false
        mirrors:
            <mirror-host-name>: <mirror-local-pathname>
        objectstore:
            backend:
                type: [local|s3|...]
                basedir: <if-local-archive-storage-path>
        index:
        database: D:\archive\litero\archive.db