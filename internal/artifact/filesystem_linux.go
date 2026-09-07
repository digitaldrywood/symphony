package artifact

import (
	"fmt"

	"golang.org/x/sys/unix"
)

const (
	afsFilesystemMagic  = 0x5346414f
	cephFilesystemMagic = 0x00c36400
	codaFilesystemMagic = 0x73757245
	ncpFilesystemMagic  = 0x0000564c
	nfsFilesystemMagic  = 0x00006969
	smbFilesystemMagic  = 0x0000517b
	smb2FilesystemMagic = 0xfe534d42
	v9FilesystemMagic   = 0x01021997
)

func validateLocalDatabaseFilesystem(directory string) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(directory, &stat); err != nil {
		return fmt.Errorf("inspect artifact catalog filesystem: %w", err)
	}
	if isNetworkFilesystemType(stat.Type) {
		return fmt.Errorf("%w: %s uses filesystem type %#x", ErrInvalid, directory, stat.Type)
	}
	return nil
}

func isNetworkFilesystemType(filesystemType int64) bool {
	switch filesystemType {
	case afsFilesystemMagic,
		cephFilesystemMagic,
		codaFilesystemMagic,
		ncpFilesystemMagic,
		nfsFilesystemMagic,
		smbFilesystemMagic,
		smb2FilesystemMagic,
		v9FilesystemMagic:
		return true
	default:
		return false
	}
}
