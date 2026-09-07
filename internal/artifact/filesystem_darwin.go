package artifact

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func validateLocalDatabaseFilesystem(directory string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(directory, &stat); err != nil {
		return fmt.Errorf("inspect artifact catalog filesystem: %w", err)
	}
	if !isLocalFilesystem(stat.Flags) {
		filesystemType := strings.TrimRight(string(stat.Fstypename[:]), "\x00")
		return fmt.Errorf("%w: %s uses %s", ErrInvalid, directory, filesystemType)
	}
	return nil
}

func isLocalFilesystem(flags uint32) bool {
	return flags&unix.MNT_LOCAL != 0
}
