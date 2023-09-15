package raftpebbledb

import (
	"github.com/cockroachdb/pebble"
)

type eventListener struct {
	log pebble.Logger
}

// BackgroundError is invoked whenever an error occurs during a background
// operation such as flush or compaction.
func (l *eventListener) BackgroundError(err error) {
	l.log.Fatalf("pebbledb background error: %s\n", err.Error())
}

// CompactionBegin is invoked after the inputs to a compaction have been
// determined, but before the compaction has produced any output.
func (l *eventListener) CompactionBegin(info pebble.CompactionInfo) {
	if info.Err != nil {
		l.log.Fatalf("pebbledb compaction begin error: %s\n", info.Err.Error())
	} else {
		l.log.Infof("pebbledb compaction begin %s\n", info.String())
	}
}

// CompactionEnd is invoked after a compaction has completed and the result
// has been installed.
func (l *eventListener) CompactionEnd(info pebble.CompactionInfo) {
	if info.Err != nil {
		l.log.Fatalf("pebbledb compaction end error: %s\n", info.Err.Error())
	} else {
		l.log.Infof("pebbledb compaction end %s\n", info.String())
	}
}

// DiskSlow is invoked after a disk write operation on a file created
// with a disk health checking vfs.FS (see vfs.DefaultWithDiskHealthChecks)
// is observed to exceed the specified disk slowness threshold duration.
func (l *eventListener) DiskSlow(info pebble.DiskSlowInfo) {
	l.log.Infof("pebbledb disk slow %s\n", info.String())
}

// FlushBegin is invoked after the inputs to a flush have been determined,
// but before the flush has produced any output.
func (l *eventListener) FlushBegin(info pebble.FlushInfo) {
	if info.Err != nil {
		l.log.Fatalf("pebbledb flush begin error: %s\n", info.Err.Error())
	} else {
		l.log.Infof("pebbledb flush begin %s\n", info.String())
	}
}

// FlushEnd is invoked after a flush has complated and the result has been
// installed.
func (l *eventListener) FlushEnd(info pebble.FlushInfo) {
	if info.Err != nil {
		l.log.Fatalf("pebbledb flush end error: %s\n", info.Err.Error())
	} else {
		l.log.Infof("pebbledb flush end %s\n", info.String())
	}
}

// ManifestCreated is invoked after a manifest has been created.
func (l *eventListener) ManifestCreated(info pebble.ManifestCreateInfo) {
	if info.Err != nil {
		l.log.Fatalf("pebbledb manifest created error: %s\n", info.Err.Error())
	} else {
		l.log.Infof("pebbledb manifest created %s\n", info.String())
	}
}

// ManifestDeleted is invoked after a manifest has been deleted.
func (l *eventListener) ManifestDeleted(info pebble.ManifestDeleteInfo) {
	if info.Err != nil {
		l.log.Fatalf("pebbledb manifest deleted error: %s\n", info.Err.Error())
	} else {
		l.log.Infof("pebbledb manifest deleted %s\n", info.String())
	}
}

// TableCreated is invoked when a table has been created.
func (l *eventListener) TableCreated(info pebble.TableCreateInfo) {
	l.log.Infof("pebbledb table created %s\n", info.String())
}

// TableDeleted is invoked after a table has been deleted.
func (l *eventListener) TableDeleted(info pebble.TableDeleteInfo) {
	if info.Err != nil {
		l.log.Fatalf("pebbledb table deleted error: %s\n", info.Err.Error())
	} else {
		l.log.Infof("pebbledb table deleted %s\n", info.String())
	}
}

// TableIngested is invoked after an externally created table has been
// ingested via a call to DB.Ingest().
func (l *eventListener) TableIngested(info pebble.TableIngestInfo) {
	if info.Err != nil {
		l.log.Fatalf("pebbledb table ingested error: %s\n", info.Err.Error())
	} else {
		l.log.Infof("pebbledb table ingested %s\n", info.String())
	}
}

// TableStatsLoaded is invoked at most once, when the table stats
// collector has loaded statistics for all tables that existed at Open.
func (l *eventListener) TableStatsLoaded(info pebble.TableStatsInfo) {
	l.log.Infof("pebbledb table stats loaded %s\n", info.String())
}

// WALCreated is invoked after a WAL has been created.
func (l *eventListener) WALCreated(info pebble.WALCreateInfo) {
	if info.Err != nil {
		l.log.Fatalf("pebbledb wal created error: %s\n", info.Err.Error())
	} else {
		l.log.Infof("pebbledb wal created %s\n", info.String())
	}
}

// WALDeleted is invoked after a WAL has been deleted.
func (l *eventListener) WALDeleted(info pebble.WALDeleteInfo) {
	if info.Err != nil {
		l.log.Fatalf("pebbledb wal deleted error: %s\n", info.Err.Error())
	} else {
		l.log.Infof("pebbledb wal deleted %s\n", info.String())
	}
}

// WriteStallBegin is invoked when writes are intentionally delayed.
func (l *eventListener) WriteStallBegin(info pebble.WriteStallBeginInfo) {
	l.log.Infof("pebbledb write stall begin %s", info.String())
}

// WriteStallEnd is invoked when delayed writes are released.
func (l *eventListener) WriteStallEnd() {
	l.log.Infof("pebbledb write stall end\n")
}
