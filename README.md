### How to run

NOTE - it only takes backup of REALTIME tables, not the offline

1. Set the following env variable

```shell
# This is the source Pinot
export PINOT_SRC_BASE_URL=http://<PINOT URL> 

# This is the destination Pinot - suppose you want to copy the source pinot to destination pinot
# If both are same i.e. you are just creating backup - do not set this
export PINOT_DEST_BASE_URL=http://<PINOT URL>

# Directory where giles will be generated
export OUT_DIR=/tmp/backup

```

2. Generate files

```shell
go run cmd/pinot/app.go
```

The <OUT_DIR> will have all the files you need