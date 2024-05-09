Running postgres:

```
docker run --rm --name pg1 -e POSTGRES_PASSWORD=test -p 5432:5432 postgres
```

then run 

```
python sqlalchemytest/__init__.py
```
