from connectors import MySQLConnector, MongoConnector, CassandraConnector


def mysql_to_mongodb(db, mysql_config, mongodb_config):
    mysql = MySQLConnector(mysql_config).use("task-manager")
    mongodb = MongoConnector(mongodb_config).create("task-manager")

    for table_name in mysql.get_tables_names():
        collection = mysql.get_collection(table_name)

        mongodb.insert_many(table_name, collection)
        print(f"Migration of table {table_name} is done!")

    mongodb.apply_foreign_keys(mysql.get_foreign_keys())
    mongodb.remove_primary_keys(mysql.get_primary_keys())
    print(f"Migration of database {db} is done!")

    del mysql
    del mongodb


def mongodb_to_cassandra(db, mongodb_config, cassandra_config):
    mongodb = MongoConnector(mongodb_config).use("task-manager")
    cassandra = CassandraConnector(cassandra_config).create("task-manager")

    for table_name in mongodb.get_tables_names():
        collection = mongodb.get_collection(table_name)

        cassandra.insert_many(table_name, collection)
        print(f"Migration of table {table_name} is done!")

    print(f"Migration of database {db} is done!")

    del mongodb
    del cassandra


if __name__ == "__main__":
    mysql_config = {"host": "localhost", "user": "root", "passwd": ""}
    mongodb_config = {"host": "localhost", "port": 27017}
    cassandra_config = {"host": "127.0.0.1", "port": 9042}

    mysql_to_mongodb("task-manager", mysql_config, mongodb_config)
    mongodb_to_cassandra("task-manager", mongodb_config, cassandra_config)
