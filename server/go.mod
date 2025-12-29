module hashbox-server

go 1.22.5

require (
	github.com/fredli74/bytearray v0.0.0-20160519123742-883b9d2bdcd6
	github.com/fredli74/cmdparser v0.0.0-20160519131828-8835ce0e2af2
	github.com/fredli74/hashbox/pkg/accountdb v0.0.0-00010101000000-000000000000
	github.com/fredli74/hashbox/pkg/core v0.0.0-00010101000000-000000000000
	github.com/fredli74/lockfile v0.0.0-20180308112638-92f5e1efe5d6
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
)

require (
	github.com/fredli74/hashbox/pkg/filelock v0.0.0-00010101000000-000000000000 // indirect
	golang.org/x/sys v0.21.0 // indirect
)

replace github.com/fredli74/hashbox/pkg/core => ../pkg/core
replace github.com/fredli74/hashbox/pkg/accountdb => ../pkg/accountdb
replace github.com/fredli74/hashbox/pkg/storagedb => ../pkg/storagedb
replace github.com/fredli74/hashbox/pkg/filelock => ../pkg/filelock
