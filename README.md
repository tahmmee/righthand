# Right Hand Checking on the Left
Data verification tool that uses DCP mutations as source to verify query data (view, index, fts).
Especially useful when rollbacks have occured, the mutations from dcp stream should have been rolled back from said index sources.

## Usage
```
# run against default bucket creating streams every 20s
./righthand -host http://172.23.106.14:8091  -bucket default -window 20
```

