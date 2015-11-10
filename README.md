```
	 ,+---+
	+---+´|    HASHBOX SOURCE
	| # | |    Copyright 2015
	+---+´
```

# Hashbox #

[Download binaries](https://bitbucket.org/fredli74/hashbox/downloads) *(only platforms that I personally test and run)*

**DISCLAIMER! This is an early Alpha version and it is still missing a lot of important features
**

Hashbox is a cross-platform derivate of a proprietary backup system (called BUP) that Magnus Lidström invented back around 2001.

* General purpose data block storage system (Hashbox)
* Efficient backup system with full data de-duplication (Hashback)
* Cross-platform with no runtime dependencies
* Open source, GO source with minimal external dependencies

## Hashbox Server ##
* Hashbox blocks are variable in length
* Each block can optionally be compressed (only zlib implemented)
* Each block has a unique 128 bit ID / hash calculated from the plain uncompressed block data and references
* Each block can refer to other blocks (used for quota calculation and GC)
* The server keeps a simple local database of accounts
* The server keeps a pure transactional database of datasets and dataset versions for each account
* Datasets contains a name, version and a reference to a "root" block ID
* Everything not referenced by a dataset are subject to GC

### Starting the server ###

Create a user

`./hashbox-freebsd-amd64 adduser <username> <password>`


Start the server

`./hashbox-freebsd-amd64 [-port=<port>] [-db=<path>]`

## Hashbox Backup Client (Hashback) ##
* Each file is split into blocks based on a rolling checksum
* Each block hash (block ID) is calculated and sent to the server
* Server requests only blocks that it does not already have from the client, this in combination with the rollsum splitting allows the server to only request part of files that was not previously backed up
* File metadata such as file name, size, modification time and attributes are stored in a directory block
* A tree of directory blocks are then saved as a dataset version to the server
* Incremental backups are done by downloading and comparing the directory blocks from the last backup with the current files on disk. This allows for very fast file hash skipping based on file name, size and date.

### Using the client ###

Setup connection options and save them as default

`./hashback -user=<username> -password=<password> -server=<ip>:<port> -progress -save-options`


Show account information

`./hashback info` 


Create a backup

`./hashback store <dataset> (<folder> | <file>)...`


Show a list of datasets

`./hashback list` 

 

### Todo (in somewhat prio order) ###
(Lots of TODOs comments inside the source)

* Change client list command so you can list into backup directories
* Client selective restore
* Client platform specific file information (User/Group on linux, system/hidden on Windows for example)
* Client data encryption
* Client dataset deletion
* Server garbage collection
* Server quota calculations and restrictions
* Server admin interface