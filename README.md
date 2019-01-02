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
1. Install go. Version 1.7 is minimum required but the newer the better.
If your distribution lacks the required version, check backports or updates (e.g. for [debian](https://packages.debian.org/search?keywords=golang) or [ubuntu](https://packages.ubuntu.com/search?keywords=golang))
Alternatively, install [manually](https://golang.org/doc/install).

2. Run the following commands:
```shell
mkdir workspace
cd workspace
export GOPATH=$(pwd)
go get github.com/dop251/diskrsync/diskrsync
sudo cp -a bin/diskrsync /usr/local/bin
```

3. Make sure the binary is copied to the remote machine as well.
If the remote machine has a different CPU or OS you may want to
use [cross-compilation](https://dave.cheney.net/2015/08/22/cross-compilation-with-go-1-5).
For example if you are want to build a binary for ARM:
```shell
GOARCH=arm go get github.com/dop251/diskrsync/diskrsync
ls -l bin/linux_arm/diskrsync
```


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
