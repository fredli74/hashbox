```
	 ,+---+
	+---+´|    HASHBOX SOURCE
	| # | |    Copyright 2015
	+---+´
```

# Hashbox #

Hashbox is a Content Addressable Storage server that is a derivate of a proprietary backup system (called BUP) that Magnus Lidström invented back around 2001.

**DISCLAIMER! This is an early Alpha version and it is still missing a lot of important features
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

### Todo (in somewhat prio order) ###
(Lots of TODOs comments inside the source)
* Change client list command so you can list into backup directories
* Client selective restore
* Decision : When splitting data, should the minimum block-size be based on the full data size?)
* Decision : What is the max size of a block
* Client platform specific file information (User/Group on linux, system/hidden on Windows for example)
* Decision: Client compression algorithm, using zlib today
* Client data encryption
* Server garbage collection
* Server quota calculations and restrictions
* Server admin interface