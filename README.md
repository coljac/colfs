# colfs

This trivial utility archives files without compression, and makes it easy to get them in and out. It's for situations where you have many files infrequently accessed. For instance, in pseudocode:

```
for id in list_of_ids:
    colfs --archive bigfiles.colfs restore <id> # will copy id/* to the file system
    process_files(id)
    colfs --archive bigfiles.colfs dehydrate # will delete any files that are safe in the archive
```

Help:

```
colfs is a CLI tool for combining multiple files into a single blob with an index,
and for managing and restoring files from these blobs.

Examples:
  colfs create --archive myfiles.colfs file1.txt file2.txt dir/file3.txt
  colfs dir --archive myfiles.colfs
  colfs restore --archive myfiles.colfs "*.txt"
  colfs dehydrate --archive myfiles.colfs

Usage:
  colfs [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  create      Concatenate files into a blob with an index
  dehydrate   Delete local files that exist in the blob
  dir         List contents of a blob
  help        Help about any command
  restore     Restore files matching the pattern
  update      Add or update files in an existing archive

Flags:
  -a, --archive string   Archive file (can also be set via COLFS_ARCHIVE environment variable)
  -h, --help             help for colfs
  -q, --quiet            Suppress output messages

Use "colfs [command] --help" for more information about a command.
```


