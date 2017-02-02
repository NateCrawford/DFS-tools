# DFS-tools
Tools for working with Distributed File Systems

Mostly useful on Greenplanet (UC Irvine Physical Sciences cluster computer) dealing with our Lustre and BeeGFS systems.

## Archival tools
Tar2DFS.sh - Takes a generic user directory and attempts to condense trees with MANY entities into chunked tar files. MD5 checksums are calculated and stored for the original files. This significantly reduces the time and space of dealing with metadata.

CheckDFS.sh - Operates on the destination directory of Tar2DFS.sh. Checks the checksums of the tarred files in place.

Both are done in parallel using the [GNU Parallel tool] (https://www.gnu.org/software/parallel/):

O. Tange (2011): GNU Parallel - The Command-Line Power Tool, ;login: The USENIX Magazine, February 2011:42-47.
