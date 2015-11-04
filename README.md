	 ,+---+
	+---+´|    HASHBOX SOURCE
	| # | |    Copyright 2015
	+---+´

# Hashbox #

Hashbox is a Content Addressable Storage server that is a derivate of a proprietary backup system (called BUP) that Magnus Lidström invented back around 2001.

**DISCLAIMER! This is an early Alpha version and it is still missing a lot of important features (see Roadmap section)
**

### Hashbox Backup Client (Hashback) ###

...

### Key concepts ###

* General purpose data block storage system (blob)
* Efficient backup system with full data de-duplication
* Small footprint, low memory usage on both server and client
* Cross-platform with no runtime dependencies
* Open source, GO source with minimal external dependencies

### How do I get set up? ###

* Compile and run

### Roadmap ###

* Add client data encryption
* Compare client compression (using zlib today)
* Add server garbage collection
* Add client list into backups
* Add client selective restore
* ... more