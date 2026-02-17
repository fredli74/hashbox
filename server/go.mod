module hashbox-server

go 1.26.0

require (
	github.com/fredli74/bytearray v0.0.0-20160519123742-883b9d2bdcd6
	github.com/fredli74/cmdparser v0.0.0-20260217142914-ddb3791f9fab
	github.com/fredli74/hashbox/pkg/accountdb v0.0.0-00010101000000-000000000000
	github.com/fredli74/hashbox/pkg/core v0.0.0-00010101000000-000000000000
	github.com/fredli74/hashbox/pkg/storagedb v0.0.0-00010101000000-000000000000
	github.com/fredli74/lockfile v0.0.0-20180308112638-92f5e1efe5d6
)

require (
	github.com/fredli74/hashbox/pkg/lockablefile v0.0.0-00010101000000-000000000000 // indirect
	golang.org/x/sys v0.21.0 // indirect
)

replace (
	github.com/fredli74/hashbox/pkg/accountdb => ../pkg/accountdb
	github.com/fredli74/hashbox/pkg/core => ../pkg/core
	github.com/fredli74/hashbox/pkg/lockablefile => ../pkg/lockablefile
	github.com/fredli74/hashbox/pkg/storagedb => ../pkg/storagedb
)
