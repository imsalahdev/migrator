import mysql.connector
import base64
from uuid import uuid4
from pymongo import MongoClient
from bson.objectid import ObjectId
from cassandra import cluster
from utils import sanitize_string


class MySQLConnector:
    """This class implements all the methods needed to migrate from a mysql database."""

    def __init__(self, mysql_config: dict):
        """@param {dictionary} mysql_config - The connection configuration of mysql."""

        self.connection = mysql.connector.connect(
            host=mysql_config["host"],
            user=mysql_config["user"],
            passwd=mysql_config["passwd"],
        )
        self.cursor = self.connection.cursor()
        self.primary_keys = set()

    def use(self, db_name: str) -> "MySQLConnector":
        """This method selects the database to use.
        @param {string} db_name - The database name to select.
        @return {MySQLConnector} - Returns the current object."""

        self.db_name = db_name
        self.cursor.execute(f"USE `{self.db_name}`")
        return self

    def create(self, db_name: str) -> "MySQLConnector":
        """This method creates a new database if it already existed.
        @param {string} db_name - The database name to create.
        @return {MySQLConnector} - Returns the current object."""

        if self.db_exists(db_name):
            db_name += f"_{uuid4().hex[:8]}"
        self.cursor.execute(f"CREATE DATABASE `{db_name}`")
        return self.use(db_name)

    def db_exists(self, db_name: str) -> bool:
        """This method checks if a database's already existing.
        @param {string} db_name - The database name to create.
        @return {boolean} - Returns if database's already existing."""

        self.cursor.execute(f"SHOW DATABASES")
        return any([name == db_name for (name,) in self.cursor.fetchall()])

    def get_tables_name(self) -> list:
        """This method gets all table names of the current database.
        @return {list} - Returns a list of table names."""

        self.cursor.execute("SHOW TABLES")
        return [name for (name,) in self.cursor.fetchall()]

    def get_results(self, table_name: str) -> list:
        """This method fetches all rows of a specified table.
        @param {string} table_name - The table name to fetch from.
        @return {list} - Returns a list of rows of a certain table."""

        self.cursor.execute(f"SELECT * FROM {table_name}")
        return self.cursor.fetchall()

    def get_columns_info(self, table_name: str) -> list:
        """This method fetches all columns information of a specified table.
        @param {string} table_name - The table name to fetch from.
        @return {list} - Returns a list of columns names and types."""

        self.cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns_infos = []
        for rs in self.cursor.fetchall():
            columns_infos.append((rs[0], rs[1]))
            if rs[3] == "PRI":
                self.primary_keys.add((table_name, rs[0]))
        return columns_infos

    def get_primary_keys(self) -> set:
        """This method gets all the primary keys of the current database.
        @return {list} - Returns a list of primary keys."""

        return self.primary_keys

    def get_foreign_keys(self) -> list:
        """This method gets all the foreign keys of the current database.
        @return {list} - Returns a list of foreign keys."""

        self.cursor.execute(
            f"""
            SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE REFERENCED_TABLE_SCHEMA = '{self.db_name}' 
            AND CONSTRAINT_NAME IN (
                SELECT CONSTRAINT_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                WHERE CONSTRAINT_SCHEMA = '{self.db_name}' 
                AND CONSTRAINT_TYPE = 'FOREIGN KEY'
            )
        """
        )
        return self.cursor.fetchall()

    def get_table(self, table_name: str) -> list:
        """This method gets all table values from a table name.
        @param {string} table_name - The table name to fetch from.
        @return {list} - Returns table values from a table name."""

        table = []
        for result in self.get_results(table_name):
            record = {}
            for (i, (field, type)) in enumerate(self.get_columns_info(table_name)):
                if type.startswith("tinyint"):
                    record[field] = result[i] == 1
                elif type.startswith("longblob"):
                    record[field] = base64.encodebytes(result[i])
                elif type.startswith("date"):
                    record[field] = str(result[i])
                else:
                    record[field] = result[i]
            table.append(record)
        return table

    def __del__(self):
        """This method closes the cursor and the connection objects."""

        self.cursor.close()
        self.connection.close()


class MongoConnector:
    """This class implements all the methods needed to migrate from and to a mongodb database."""

    def __init__(self, mongodb_config: dict):
        """@param {dictionary} mongodb_config - The connection configuration of mongodb."""

        self.client = MongoClient(
            host=mongodb_config["host"], port=mongodb_config["port"]
        )

    def use(self, db_name: str) -> "MongoConnector":
        """This method selects the database to use.
        @param {string} db_name - The database name to select.
        @return {MongoConnector} - Returns the current object."""
        if self.db_exists(db_name):
            self.db_name = db_name
            self.db = self.client[db_name]
            return self
        else:
            raise Exception(f"1049 (42000): Unknown database '{db_name}'")

    def create(self, db_name: str) -> "MongoConnector":
        """This method creates a new database if it already existed.
        @param {string} db_name - The database name to create.
        @return {MongoConnector} - Returns the current object."""

        if self.db_exists(db_name):
            db_name += f"_{uuid4().hex[:8]}"
        self.db_name = db_name
        self.db = self.client[db_name]
        return self

    def db_exists(self, db_name: str) -> bool:
        """This method checks if a database's already existing.
        @param {string} db_name - The database name to create.
        @return {boolean} - Returns if database's already existing."""

        return db_name in self.client.list_database_names()

    def insert_many(self, table_name: str, table: list) -> bool:
        """This method inserts values into the specified collection.
        @param {string} table_name - The table name.
        @param {list} table - The table values.
        @return {boolean} - Returns if insertion has completed successfully."""

        if len(table) != 0:
            self.db[table_name].insert_many(table)
            return True
        return False

    def apply_foreign_keys(self, foreign_keys: list) -> None:
        """This method apply foreign keys relationships.
        @param {list} foreign_keys - A list of foreign keys names."""
        for (
            table_name,
            column_name,
            referenced_table_name,
            referenced_column_name,
        ) in foreign_keys:
            for doc in self.db[table_name].find():
                referenced_table = self.db[referenced_table_name].find_one(
                    {referenced_column_name: doc[column_name]}
                )

                if referenced_table != None:
                    self.db[table_name].update(
                        {column_name: doc[column_name]},
                        {"$set": {column_name: referenced_table["_id"]}},
                    )

        self.rename_fields_from_foreign_keys(foreign_keys)

    def rename_fields_from_foreign_keys(self, foreign_keys: list) -> None:
        """This method renames fields from foreign keys.
        @param {list} foreign_keys - A list of foreign keys names."""

        for (table_name, column_name, referenced_table_name, *_rest) in foreign_keys:
            new_column_name = f"{referenced_table_name}_id"
            if column_name != new_column_name:
                self.db[table_name].update_many(
                    {}, {"$rename": {column_name: new_column_name}}
                )

    def remove_primary_keys(self, primary_keys: list) -> None:
        """This method removes all primary keys fields.
        @param {list} primary_keys - A list of primary keys names."""

        for (table_name, primary_key) in primary_keys:
            self.db[table_name].update_many({}, {"$unset": {primary_key: 1}})

    def get_collection_names(self) -> list:
        """This method gets all collection names of the current database.
        @return {list} - Returns a list of collection names."""

        return self.db.collection_names()

    def get_results(self, table_name: str) -> list:
        """This method fetches all documents of a specified table name.
        @param {string} table_name - The table name to fetch from.
        @return {list} - Returns a list of documents of a certain table name."""

        return self.db[table_name].find()

    def get_documents_info(self, table_name: str) -> list:
        """This method fetches all documents information of a specified table name.
        @param {string} table_name - The table name to fetch from.
        @return {list} - Returns a list of documents names and types."""

        return self.db[table_name].find_one().keys()

    def get_collection(self, table_name: str) -> list:
        """This method gets all collection values from a table name.
        @param {string} table_name - The table name to fetch from.
        @return {list} - Returns collection values from a table name."""

        collection = []
        for result in self.get_results(table_name):
            document = {}
            for field in self.get_documents_info(table_name):
                document[field] = result[field]
            collection.append(document)
        return collection

    def __del__(self):
        """This method closes the client object."""

        self.client.close()


class CassandraConnector:
    """This class implements all the methods needed to migrate to a cassandra database."""

    def __init__(self, cassandra_config: dict):
        """@param {dictionary} cassandra_config - The connection configuration of cassandra."""

        self.cluster = cluster.Cluster(
            [cassandra_config["host"]], port=cassandra_config["port"]
        )
        self.session = self.cluster.connect()

    def use(self, keyspace_name: str) -> "CassandraConnector":
        """This method selects the keyspace to use.
        @param {string} keyspace_name - The keyspace name to select.
        @return {CassandraConnector} - Returns the current object."""

        if self.keyspace_exists(keyspace_name):
            self.session.execute(f"USE {self.keyspace_name}")
            return self
        return None

    def create(self, keyspace_name: str) -> "CassandraConnector":
        """This method creates a new keyspace if it already existed.
        @param {string} keyspace_name - The keyspace name to select.
        @return {CassandraConnector} - Returns the current object."""

        if self.keyspace_exists(keyspace_name):
            self.keyspace_name += f"_{uuid4().hex[:8]}"

        self.session.execute(
            f"CREATE KEYSPACE {self.keyspace_name} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1}}"
        )
        return self.use(self.keyspace_name)

    def keyspace_exists(self, keyspace_name: str) -> bool:
        """This method checks if a keyspace's already existing.
        @param {string} keyspace_name - The keyspace name to create.
        @return {boolean} - Returns if keyspace's already existing."""

        self.keyspace_name = sanitize_string(keyspace_name)
        keyspace_names = self.session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces"
        )
        return any([rs.keyspace_name == self.keyspace_name for rs in keyspace_names])

    def get_collection_types(self, document: dict) -> dict:
        """This method fetches all types from a document.
        @param {dict} document - The document to fetch from.
        @return {dict} - Returns a dictionary of field names and their types."""

        return {sanitize_string(key): type(document[key]) for key in document.keys()}

    def create_table(self, collection_name: str, collection_types: dict) -> None:
        """This method creates a table.
        @param {string} collection_name - The table name.
        @param {dict} collection_types - The table field's types."""

        types = []
        for field_name in collection_types.keys():
            field_type = collection_types[field_name]
            if field_type == ObjectId:
                if field_name == "id":
                    types.append("id text PRIMARY KEY")
                else:
                    types.append(f"{field_name} text")
            elif field_type == int:
                types.append(f"{field_name} int")
            elif field_type == float:
                types.append(f"{field_name} float")
            elif field_type == bool:
                types.append(f"{field_name} boolean")
            elif field_type == bytes:
                types.append(f"{field_name} blob")
            else:
                types.append(f"{field_name} text")

        types_str = ", ".join(types)
        self.session.execute(f"CREATE TABLE {collection_name} ({types_str})")

    def insert_many(self, collection_name: str, collection: list) -> None:
        """This method inserts values into the specified table.
        @param {string} collection_name - The table name.
        @param {list} collection - The table values."""

        collection_types = self.get_collection_types(collection[0])
        self.create_table(collection_name, collection_types)
        for document in collection:
            altered_document = {}
            for key in document.keys():
                altered_document[sanitize_string(key)] = document[key]
                if type(document[key]) == ObjectId:
                    altered_document[sanitize_string(key)] = str(document[key])

            altered_document_keys = altered_document.keys()
            column_names = ", ".join(altered_document_keys)
            column_values = ", ".join(f"%({key})s" for key in altered_document_keys)

            self.session.execute(
                f"INSERT INTO {collection_name} ({column_names}) VALUES ({column_values})",
                altered_document,
            )
