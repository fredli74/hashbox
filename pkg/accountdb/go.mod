module github.com/fredli74/hashbox/pkg/accountdb

go 1.22.5

require (
	github.com/fredli74/hashbox/pkg/core v0.0.0-00010101000000-000000000000
	github.com/fredli74/hashbox/pkg/filelock v0.0.0-00010101000000-000000000000
)

require (
	github.com/fredli74/bytearray v0.0.0-20160519123742-883b9d2bdcd6 // indirect
	golang.org/x/sys v0.21.0 // indirect
)

replace github.com/fredli74/hashbox/pkg/core => ../core
replace github.com/fredli74/hashbox/pkg/filelock => ../filelock
