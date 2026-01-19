module hashback

go 1.25.6

require (
	github.com/fredli74/bytearray v0.0.0-20160519123742-883b9d2bdcd6
	github.com/fredli74/cmdparser v0.0.0-20260119100722-3ee0dbb0354a
	github.com/fredli74/hashbox/pkg/core v0.0.0-00010101000000-000000000000
	github.com/fredli74/lockfile v0.0.0-20180308112638-92f5e1efe5d6
	github.com/smtc/rollsum v0.0.0-20150721100732-39e98d252100
)

replace (
	github.com/fredli74/hashbox/pkg/core => ../pkg/core
)
