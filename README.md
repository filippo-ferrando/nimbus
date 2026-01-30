# nimbus

parallel file transfer that uses `cp` and `rsync` to copy files from:

- local <-> local
- remote <-> local
- local <-> remote
useful to transfer big file with poor internet connection or session limitations

> Always check if the file has been copied correctly using checksums (md5sum, sha256sum, etc)
> Automatic checksum is although implemented, it is recommended to verify manually

## Features

- file chunking
- resume after failing
- only 1 password prompt for remote transfers
- parallel transfers
- clear logging (do not trust elapsed time ;-) )
- statistics at the end of the transfer

This script is aimed to transfer big files via an SSH connection that can be unstable or frequently dropped, a high number of blocks can help to resume the transfer from the last successful block instead of starting over again at the cost of some overhead (packet reconstruction, more hash calculations, etc)
