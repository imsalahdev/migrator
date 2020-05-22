# migrator
This is a school project that aims to allow migration from both *mysql* to *mongodb* and *mongodb* to *cassandra* databases.

## Setup

1. Clone the repo

   ```shell
   git clone https://github.com/imsalahdev/migrator
   cd migrator
   ```

2. Install the dependencies

   ```shell
   pip install -r requirements.txt
   ```

## Code Simple

### Migration from *MySQL* to *MongoDB*
```py
mysql_config = {"host": "localhost", "user": "root", "passwd": ""}
mongodb_config = {"host": "localhost", "port": 27017}

mysql_to_mongodb("task-manager", mysql_config, mongodb_config)
```
### Migration from *MongoDB* to *Cassandra*
```py
mongodb_config = {"host": "localhost", "port": 27017}
cassandra_config = {"host": "127.0.0.1", "port": 9042}

mongodb_to_cassandra("task-manager", mongodb_config, cassandra_config)
```

## Licence

MIT
