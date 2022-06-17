
# Veeam Pattern Benchmark

```shell
export LOCAL_ACCESS=RSTOR17NIDRFYY6P6UPLCIW7P7
export LOCAL_SECRET=3GO/EwR6DUp2whX8njcz/6mzHyzuvY7GrArIgBllClW
export LOCAL_S3=http://127.0.0.1:32005

go run veeam-pattern.go $LOCAL_S3 $LOCAL_ACCESS $LOCAL_SECRET

# run without argument to see help
```

Example output:

```shell
# default settings:
PUT   1331 (22.2/s, 0 ERR)
GET   2650 (44.1/s, 0 ERR)
LIST   743 (12.4/s, 0 ERR, 72454 rows, 1206.7 rows/s)
DEL   1644 (27.4/s, 0 ERR)

go run veeam-pattern.go $LOCAL_S3 $LOCAL_ACCESS $LOCAL_SECRET -P 100 -G 50 -L 10 -D 10
PUT  170161 (2835.4/s, 0 ERR)
GET  268065 (4467.4/s, 0 ERR)
LIST  4925 (82.0/s, 0 ERR, 1233287 rows, 20529.0 rows/s)
DEL  19634 (327.2/s, 0 ERR)
```
