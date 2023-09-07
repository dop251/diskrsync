diskrsync -- rsync for block devices and disk images
===

This is a utility for remote backup of disk image files or devices. It uses ssh as a transport
and tries to minimise the amount of data transferred. This is done by building a Merkle tree of
[blake2](https://blake2.net/) hashes on both the source and the target hosts and then
traversing the tree only transmitting those blocks where the hashes differ.

It is important that the source file does not change during the process. Typically this would
be achieved by creating an LVM snapshot or by freezing the VM if that's not possible.

By default the resulting file is compressed using the [spgz](https://github.com/dop251/spgz) library (this can be disabled by
using --no-compress flag). Note this only works on filesystems that support punching holes in
files like xfs or ext4.

The utility handles sparse files (or just files with a lot of zeros) efficiently. The resulting
file will be sparse (even if not compressed).

Size changes are also supported (both shrinks and expansions).

Installation
---
1. Install go. Version 1.16 is minimum required but the newer the better.
If your distribution lacks the required version, check backports or updates (e.g. for [debian](https://packages.debian.org/search?keywords=golang) or [ubuntu](https://packages.ubuntu.com/search?keywords=golang))
Alternatively, install [manually](https://golang.org/doc/install).

2. Run the following commands:
```shell
mkdir workspace
cd workspace
GOPATH=$(pwd) go install github.com/dop251/diskrsync/diskrsync@latest
sudo cp -a bin/diskrsync /usr/local/bin
```

3. Make sure the binary is copied to the remote machine as well.
If the remote machine has a different CPU or OS you may want to
use [cross-compilation](https://dave.cheney.net/2015/08/22/cross-compilation-with-go-1-5).
For example if you are want to build a binary for ARM:
```shell
GOPATH=$(pwd) GOARCH=arm go install github.com/dop251/diskrsync/diskrsync@latest
ls -l bin/linux_arm/diskrsync
```

Usage
---
`diskrsync [--ssh-flags="..."] [--no-compress] [--calc-progress] [--sync-progress] [--verbose] [{--source|--target}] <src> <dst>`
- ssh-flags: additional args for ssh (example: `--ssh-flags="-i ~/.ssh/backup.key"`)
- no-compress: do not compress the target image. This flag is required if the target is a block device.
- calc-progress: display prograss bar for hash calulation phase
- sync-progress: display progress bar for synchronisation phase
- verbose: display both progress bars and additional info on block size and transferred data
- source: (default) diskrsync is the source and it connects to the remote target
- target: diskrsync connects the remote source
- src/dst: `[[user@]host:]path` local or remote file/device

Usage examples
---

```shell
diskrsync /dev/vg00/lv_snap ruser@backuphost:/mnt/backup/disk
```

This ensures that /mnt/backup/disk is up-to-date with the LV snapshot. The file will be compressed
using spgz and can be recovered using the following command:

```shell
spgz -x /mnt/backup/disk /dev/vg00/....
```



```shell
diskrsync --verbose --no-compress --ssh-flags="-i id_file" /var/lib/libvirt/images/disk.img ruser@rbackuphost:/mnt/backup/
```

This ensures that /mnt/backup/disk.img is up-to-date with the source file.


```shell
diskrsync --verbose --no-compress /dev/mapper/vg0-snap_vm-101-disk-0 user@remotehost:/dev/rbd7
```

This command transfers local source block device (lvm snapshot) to remote block device (rados block device in the example).
