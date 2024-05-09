Running postgres:

```
docker run --rm --name pg1 -e POSTGRES_PASSWORD=test -p 5432:5432 postgres
```

then run 

```
python sqlalchemytest/__init__.py
```


## Results 

```
generating data
done generating data
[version1] time seconds: 47.20304799079895
[version1] min memory: 0.00 MiB
[version1] max memory: 4091.36 MiB
[version1] record count: 2000000
--------------------------------------------------------------------------------
[version2] time seconds: 58.38125801086426
[version2] min memory: 0.00 MiB
[version2] max memory: 593.08 MiB
[version2] record count: 2000000
--------------------------------------------------------------------------------
[version3] time seconds: 10.627463817596436
[version3] min memory: -272.75 MiB   <-- not sure how to solve this, a few gc.collect()'s don't seem to be reliable
[version3] max memory: 984.27 MiB
[version3] record count: 2000000
--------------------------------------------------------------------------------
[version4] time seconds: 9.829507827758789
[version4] min memory: 0.00 MiB
[version4] max memory: 1071.83 MiB
[version4] record count: 2000000
--------------------------------------------------------------------------------
[version5] time seconds: 9.967904090881348
[version5] min memory: 0.00 MiB
[version5] max memory: 1.86 MiB
[version5] record count: 2000000
```
