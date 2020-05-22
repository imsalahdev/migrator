from connectors import MySQLConnector, MongoConnector, CassandraConnector


def mysql_to_mongodb(schema_name: str, mysql_config: dict, mongodb_config: dict) -> None:
    """This procedure handles database migration from mysql to mongodb.
    @param {string} schema_name - The schema to migrate from and to.
    @param {dictionary} mysql_config - The connection configuration of mysql.
    @param {dictionary} mongodb_config - The connection configuration of mongodb."""

    try:
        mysql = MySQLConnector(mysql_config).use(schema_name)
        mongodb = MongoConnector(mongodb_config).create(schema_name)

        print(f"Migration {{MySQL => MongoDB}} of `{schema_name}` started:")
        for table_name in mysql.get_tables_name():
            table = mysql.get_table(table_name)
            if mongodb.insert_many(table_name, table):
                print(" " * 10 + "✓ " + table_name)
            else:
                print(" " * 10 + "🞩 " + table_name)

        mongodb.apply_foreign_keys(mysql.get_foreign_keys())
        mongodb.remove_primary_keys(mysql.get_primary_keys())
        print(f"Migration {{MySQL => MongoDB}} finished!\n")

        del mysql
        del mongodb
    except Exception as e:
        print(f"Error: {str(e)}")


def mongodb_to_cassandra(schema_name: str, mongodb_config: dict, cassandra_config: dict) -> None:
    """This procedure handles database migration from mongodb to cassandra.
    @param {string} schema_name - The schema to migrate from and to.
    @param {dictionary} mongodb_config - The connection configuration of mongodb.
    @param {dictionary} cassandra_config - The connection configuration of cassandra."""

    try:
        mongodb = MongoConnector(mongodb_config).use(schema_name)
        cassandra = CassandraConnector(cassandra_config).create(schema_name)

        print(
            f"Migration {{MongoDB => Cassandra}} of `{schema_name}` started:")
        for collection_name in mongodb.get_collection_names():
            collection = mongodb.get_collection(collection_name)
            cassandra.insert_many(collection_name, collection)
            print(" " * 10 + "✓ " + collection_name)

        print(f"Migration {{MongoDB => Cassandra}} finished!\n")

        del mongodb
        del cassandra
    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    mysql_config = {"host": "localhost", "user": "root", "passwd": ""}
    mongodb_config = {"host": "localhost", "port": 27017}
    cassandra_config = {"host": "127.0.0.1", "port": 9042}

    mysql_to_mongodb("task-manager", mysql_config, mongodb_config)
    mongodb_to_cassandra("task-manager", mongodb_config, cassandra_config)
