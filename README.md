```
	 ,+---+
	+---+´|    HASHBOX SOURCE
	| # | |    Copyright 2015
	+---+´
```

# Hashbox #

[Download binaries](https://bitbucket.org/fredli74/hashbox/downloads) *(only binaries for platforms that I personally test and run)*

**DISCLAIMER! This is an early Alpha version and it is still missing some features
**

Hashbox is a cross-platform derivate of a proprietary backup system (called BUP) that Magnus Lidström invented back around 2001.

* General purpose data block storage system (Hashbox)
* Efficient backup system with full data de-duplication (Hashback)
* Cross-platform GO open source with minimal external dependencies
* Single binaries with no runtime dependencies

## Hashbox Server ##
* Hashbox blocks are variable in length
* Each block can optionally be compressed (only zlib implemented)
* Each block has a unique 128 bit ID / hash calculated from the plain uncompressed block data and references
* Each block can refer to other blocks (used for GC)
* The server keeps a simple local database of accounts
* The server keeps a pure transactional database of datasets and dataset versions for each account
* Datasets contains a name, version and a reference to a "root" block ID
* Everything not referenced by a dataset are subject to GC

### Starting the server ###

**Create a user**

`./hashbox-freebsd-amd64 adduser <username> <password>`


**Start the server**

`./hashbox-freebsd-amd64 [-port=<port>] [-data=<path>] [-index=<path>]`

Optional arguments data and index will tell the server where to keep all data and index/metadata files. If possible the index path should be put on fast storage such as SSD as they are used for every single access. These files can also be recreated from the data files by running the -repair command.


**Run a garbage collect (GC)**

`./hashbox-freebsd-amd64 gc [-index]`

Optional argument index will just run the mark and sweep on the index file. After the sweep has completed, a list of how much unused data (dead data) there is in each storage file will be displayed. Running without the index option will do a full compact on all datafiles freeing up unused space.


**Run a storage file check**

`./hashbox-freebsd-amd64 check-storage [-repair] [-skipdata] [-skipmeta] [-skipindex]`

Optional arguments skipdata, skipmeta and skipindex can skip checks to speed up the procedure. If repair is specified it will recreate metadata and indexes from blocks found in data files. Bare in mind that doing this after an index GC will re-add the indexes as alive blocks again.


## Hashbox Backup Client (Hashback) ##
* Each file is split into blocks based on a rolling checksum
* Each block hash (block ID) is calculated and sent to the server
* Server requests only blocks that it does not already have, this in combination with the rollsum splitting allows the server to only request parts of files that were not previously stored
* File metadata such as file name, size, modification time and attributes are stored in a directory block
* A tree of directory blocks are then saved as a dataset version to the server
* During backup a full file list is saved locally in a cache file. This file is used for reference during the next incremental backup. This allows for fast file skipping based on file name, size and date.
* Incremental backups are done by always checking the root block ID of the last backup. If this is the same as the local cache file, the cache file will be used, otherwise the full file reference list is downloaded from the server.
* A standard set of platform-specific files to ignore is included, addtional files to ignore can be added with the -ignore option
* Optional retention of old backups allows you to keep weekly and daily backups for a specified duration (backups made the past 24 hours are always kept)

### Using the client ###

**Setup connection options and save them as default**

`./hashback -user=<username> -password=<password> -server=<ip>:<port> -progress -saveoptions`


**Add installation specific files to ignore**

`hashback.exe -ignore=D:\temp -showoptions -saveoptions`

Ignore is case sensitive (even on Windows platform) so make sure it matches with the local files. Ignore pattern can contain `*` to match any number of characters, `?` to match one character or `[a-z]` to match a range of characters. An ignore pattern ending with a path separator will only match directories.


**Show account information**

`./hashback info` 


**Create a backup**

`./hashback -retaindays=7 -retainweeks=10 store <dataset> (<folder> | <file>)...`

In the example above, all backups for the past 24 hours will be kept, 1 backup per day for 7 days and 1 backup per week for 10 weeks. Everything else will be removed after a successfull backup.


**Run continuous backup**

`./hashback -interval=60 store <dataset> (<folder> | <file>)...`

In the example above, a new backup will be made every 60 minutes. If an error occurs during backup (even a disconnect), hashback will exit with an error code.


**Show a list of datasets or list files inside a dataset**

`./hashback list <dataset> [(<backup id>|.) ["<path>"]]`

If `.` is used as backup id then the last backup will be listed (or restored)


**Restore a file**

`./hashback restore <dataset> (<backup id>|.) ["<path>"...] <dest-folder>`


**Run a filediff to compare local files to stored files**

`./hashback diff <dataset> (<backup id>|.) ["<path>"...] <local-folder>`


**Manually remove a backup id from a dataset**

`./hashback remove <dataset> <backup id>`

If the last backup id of a dataset is removed, the dataset will no longer be listed


### Roadmap (Todo in somewhat prio order) ###

* Server low storage space threshold, return error on store before hitting 0 free space
* Client platform specific file information (User/Group on linux, system/hidden on Windows for example). Client should store a root block with platform information
* Server admin interface (API?) to adduser and change password so it can be done online
* Server GC scheduled or triggered on low free space
* Server GC mark and sweep partially or fully online
* Server GC compact phase online through the storage engine
* Server mirroring
* Client GUI
* Client data encryption
* Server quota calculations and restrictions (combine it with GC index mark phase?)
