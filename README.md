```
	 ,+---+    
	+---+´|    HASHBOX / HASHBACK
	| # | |    Copyright 2015-2016 Fredrik Lidström
	+---+´     
```

# Hashbox #

[Download binaries](https://github.com/fredli74/hashbox/releases/latest) *(only binaries for platforms that I personally test and run)*

**DISCLAIMER! This project is in beta stage and it should not be used for important production**

[![Build Status](https://semaphoreci.com/api/v1/fredli74/hashbox/branches/master/badge.svg)](https://semaphoreci.com/fredli74/hashbox)

Hashbox is a cross-platform derivate of a proprietary backup system (called BUP) that Magnus Lidström invented back around 2001.

### Design Goals
* General purpose data block storage system (Hashbox)
* Efficient backup system with full data de-duplication (Hashback)
* Cross-platform GO open source with few dependencies on external libraries
* Single binaries with no runtime dependencies
* Permissive MIT License

## Hashbox Server ##
* Hashbox blocks are variable in length
* Each block can optionally be compressed (only zlib implemented)
* Each block has a unique 128 bit ID / hash calculated from the plain uncompressed block data and references
* Each block can refer to other blocks (used for GC)
* The server keeps a simple local database of accounts
* The server keeps a pure transactional database of datasets and dataset versions for each account
* Datasets contains a name, version and a reference to a "root" block ID
* Everything not referenced by a dataset is subject to GC

### Starting the server ###

**Create a user**

`./hashbox-freebsd-amd64 adduser <username> <password>`


**Start the server**

`./hashbox-freebsd-amd64 [-port=<port>] [-data=<path>] [-index=<path>]`

Optional arguments `data` and `index` will tell the server where to keep all data and index/metadata files. If possible, the index files should be placed on fast storage such as SSD as they are used in every single access. These files can also be recreated from the data files by running the -repair command.


**Run a garbage collect (GC)**

`./hashbox-freebsd-amd64 gc [-compact]`

Optional argument `compact` will run the compact phase on data and meta files, freeing up unused space. After the sweep has completed, a report will be displayed on how much unused data (dead data) there is in each storage file. Running without the compact option will only sweep the index files (displaying the report, but not actually freeing any disk space).


**Run a storage file check**

`./hashbox-freebsd-amd64 check-storage [-repair] [-skipdata] [-skipmeta] [-skipindex]`

Optional arguments `skipdata`, `skipmeta` and `skipindex` can skip checks to speed up the procedure. If repair is specified it will recreate metadata and indexes from blocks found in data files. Bear in mind that doing this after an index GC will re-add the indexes as alive blocks again.


## Hashbox Backup Client (Hashback) ##
* Each file is split into blocks based on a rolling checksum.
* Each block hash (block ID) is calculated and sent to the server.
* Server requests only blocks that it does not already have. In combination with the rollsum splitting this allows the server to only request parts of files that were not previously stored.
* File metadata such as file name, size, modification time and attributes are stored in a directory block.
* A tree of directory blocks are then saved as a dataset version to the server.
* During backup, a full file list is saved locally in a cache file. This cache is used as a reference during the next incremental backup, allowing fast file skipping based on file name, size and date.
* Incremental backups are done by always checking the root block ID of the last backup. If the ID is the same as the local cache file, the cache will be used, otherwise the full file reference list is downloaded from the server.
* A standard set of platform-specific files to ignore is included. Addtional files to ignore can be added with the -ignore option.
* Optional retention of old backups allows you to keep weekly and daily backups for a specified duration (backups made in the past 24 hours are always kept).

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

In the example above, all backups for the past 24 hours will be kept, 1 backup per day for 7 days and 1 backup per week for 10 weeks. Everything else will be removed after a successful backup.


**Run continuous backup**

`./hashback -interval=60 store <dataset> (<folder> | <file>)...`

In the example above, a new backup will be made every 60 minutes. If an error occurs during backup (even a disconnect), hashback will exit with an error code.


**Show a list of datasets or list files inside a dataset**

`./hashback list <dataset> [(<backup id>|.) ["<path>"]]`

If `.` is used as backup id then the last backup will be listed.


**Restore a file**

`./hashback restore <dataset> (<backup id>|.) ["<path>"...] <dest-folder>`

If `.` is used as backup id then the last backup will be used for restoring.


**Run a filediff to compare local files to stored files**

`./hashback diff <dataset> (<backup id>|.) ["<path>"...] <local-folder>`

If `.` is used as backup id then the last backup will be used for comparing.


**Manually remove a backup id from a dataset**

`./hashback remove <dataset> <backup id>`

If the last remaining backup id of a dataset is removed, the dataset will no longer be listed.


### Roadmap ###
Things that should be implemented or considered
* Client should have a resume option on store by default (use local cache to figure out what has been backed up).
* Server low storage space threshold, return error on store before hitting 0 free space.
* Client platform specific file information (User/Group on linux, system/hidden on Windows for example). Client should store a root block with platform information.
* Server admin interface (API?) to adduser and change password so it can be done online.
* Server GC scheduled or triggered on low free space.
* Server GC mark and sweep partially or fully online.
* Server GC compact phase online through the storage engine.
* Server mirroring.
* Client GUI.
* Client data encryption (system design allows it).
* Server quota calculations and restrictions (combine it with GC index mark phase?).
