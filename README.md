```
	 ,+---+    
	+---+´|    HASHBOX / HASHBACK
	| # | |    Copyright 2015-2026 Fredrik Lidström
	+---+´     
```

# Hashbox #

[Download binaries](https://github.com/fredli74/hashbox/releases/latest) *(only binaries for platforms that I personally test and run)*

**DISCLAIMER! This project is in beta stage and it should not be used for important production**

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
* Supports Docker deployment - see [DOCKER.md](DOCKER.md) for setup instructions

### Starting the server ###

**Create a user**

`./hashbox-freebsd-amd64 adduser <username> <password>`


**Start the server**

`./hashbox-freebsd-amd64 [-port=<port>] [-data=<path>] [-index=<path>] [-loglevel=<level>]`

Optional arguments:
* `-data` and `-index` tell the server where to keep all data and index/metadata files. If possible, the index files should be placed on fast storage such as SSD as they are used in every single access. These files can also be recreated from the data files by running the recover command.
* `-loglevel` sets the logging verbosity (0=errors, 1=warnings, 2=info, 3=debug, 4=trace)

**Environment variables**

* `UMASK` - Set file creation permissions mask in octal format (default: `077` for owner-only access). Example: `UMASK=027` for group-readable files. Controls access permissions for server data files (.dat, .meta, .idx, .db, .trn).


**Run a garbage collect (GC)**

`./hashbox-freebsd-amd64 gc [-compact] [-compact-only] [-force] [-threshold=<percentage>]`

After the sweep has completed, a report will be displayed on how much unused data (dead data) there is in each storage file. Optional argument `-compact` will run the compact phase on data and meta files, freeing up unused space. Running without the compact option will only sweep the index files (displaying the report, but not actually freeing any disk space). Running with `-compact-only` will skip the sweep phase and only compact data and meta files. `-force` will ignore invalid datasets and force a garbage collect, resulting in all orphan data being removed. Use `-threshold` to set at which unused dead space threshold to compact a file.


**Run a storage file check**

`./hashbox-freebsd-amd64 verify [-content] [-repair]`

Optional arguments `-content` will verify all data content. `-repair` will invalidate broken block trees and mark dataset states invalid (this takes the storage lock).


**Run a storage recovery**

`./hashbox-freebsd-amd64 recover [start file number] [end file number]`

Recover will go through all data files and rebuild index and meta data. Optional arguments can be specified to restrict the recovery to only a range of data files.


## Hashbox Backup Client (Hashback) ##
* Each file is split into blocks based on a rolling checksum.
* Each block hash (block ID) is calculated and sent to the server.
* Server requests only blocks that it does not already have. In combination with the rollsum splitting this allows the server to only request parts of files that were not previously stored.
* File metadata such as file name, size, modification time and attributes are stored in a directory block.
* Symbolic links are preserved during backup and restore.
* A tree of directory blocks are then saved as a dataset version to the server.
* During backup, a full file list is saved locally in a cache file. This cache is used as a reference during the next incremental backup, allowing fast file skipping based on file name, size and date.
* Incremental backups are done by always checking the root block ID of the last backup. If the ID is the same as the local cache file, the cache will be used, otherwise the full file reference list is downloaded from the server.
* A standard set of platform-specific files to ignore is automatically excluded (e.g., /dev, /proc, /sys on Unix; temporary folders and Recycle Bin on Windows; system caches on macOS). Additional files to ignore can be added with the -ignore option.
* Dropbox Smart Sync placeholder files (cloud-only files) are automatically detected and skipped during backup.
* Optional retention of old backups allows you to keep yearly, weekly and daily backups for a specified duration (backups made in the past 24 hours are always kept).

### Using the client ###

**Setup connection options and save them as default**

`./hashback -user=<username> -password=<password> -server=<ip>:<port> -progress -saveoptions`


**Add installation specific files to ignore**

`hashback.exe -ignore=D:\temp -showoptions -saveoptions`

Ignore is case sensitive (even on Windows platform) so make sure it matches with the local files. Ignore pattern can contain `*` to match any number of characters, `?` to match one character or `[a-z]` to match a range of characters. An ignore pattern ending with a path separator will only match directories.


**Show account information**

`./hashback info` 


**Create a backup**

`./hashback -retaindays=7 -retainweeks=10 -retainyearly store <dataset> (<folder> | <file>)...`

In the example above, all backups for the past 24 hours will be kept, 1 backup per day for 7 days, 1 backup per week for 10 weeks, and the last backup of each year. Everything else will be removed after a successful backup.

Additional backup options:
* `-full` - Force a full (non-incremental) backup
* `-verbose` - Enable verbose output
* `-progress` - Show progress information
* `-paint` - Draw symbols for each block operation (* = uploaded, - = skipped, space = unchanged)
* `-pid=<filename>` - Create a PID/lock file to prevent multiple simultaneous backups


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
* Client platform specific file information (User/Group on linux, system/hidden on Windows for example). Client should store a root block with platform information.
* Server admin interface (API?) to adduser and change password so it can be done online.
* Server GC scheduled or triggered on low free space.
* Server GC mark and sweep partially or fully online.
* Server GC compact phase online through the storage engine.
* Server recover broken chains, the only reliable solution now is to GC and remove everything that gets unlinked
* Server mirroring.
* Client GUI.
* Client data encryption (infrastructure in place, not yet user-facing).
* Server quota calculations and restrictions (combine it with GC index mark phase?).
