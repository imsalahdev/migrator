import mysql.connector
import base64
from uuid import uuid4
from pymongo import MongoClient
from bson.objectid import ObjectId
from cassandra import cluster


class MySQLConnector:
    def __init__(self, mysql_config):
        self.connection = mysql.connector.connect(
            host=mysql_config["host"], user=mysql_config["user"], passwd=mysql_config["passwd"])
        self.cursor = self.connection.cursor()
        self.primary_keys = set()

    def use(self, db_name):
        self.db_name = db_name
        self.cursor.execute(f"USE `{self.db_name}`")
        return self

    def create(self, db_name):
        if self.db_exists(db_name):
            db_name += f"_{uuid4().hex[:8]}"
        self.cursor.execute(f"CREATE DATABASE `{db_name}`")
        return self.use(db_name)

    def db_exists(self, db_name):
        self.cursor.execute(f"show databases")
        return any([name == db_name for (name, ) in self.cursor.fetchall()])

    def get_tables_names(self):
        self.cursor.execute("SHOW TABLES")
        return [name for (name,) in self.cursor.fetchall()]

    def get_results(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        return self.cursor.fetchall()

    def get_foreign_keys_names(self):
        self.cursor.execute(f"""
            SELECT CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA = '{self.db_name}' AND CONSTRAINT_TYPE = 'FOREIGN KEY'
        """)
        return [name for (name, ) in self.cursor.fetchall()]

    def get_foreign_keys(self):
        self.get_foreign_keys_names()
        self.cursor.execute(f"""
            SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE REFERENCED_TABLE_SCHEMA = '{self.db_name}'
        """)
        return [(t[0], t[1], t[3], t[4]) for t in filter(lambda rs: rs[2] in self.get_foreign_keys_names(), self.cursor.fetchall())]

    def get_columns_infos(self, table_name):
        self.cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns_infos = []
        for rs in self.cursor.fetchall():
            columns_infos.append((rs[0], rs[1]))
            if (rs[3] == "PRI"):
                self.primary_keys.add((table_name, rs[0]))
        return columns_infos

    def get_primary_keys(self):
        return self.primary_keys

    def get_collection(self, table_name):
        collection = []
        for result in self.get_results(table_name):
            document = {}
            for (i, (field, type)) in enumerate(self.get_columns_infos(table_name)):
                if type.startswith("tinyint"):
                    document[field] = (result[i] == 1)
                elif type.startswith("longblob"):
                    document[field] = base64.encodebytes(result[i])
                else:
                    document[field] = result[i]
            collection.append(document)
        return collection

    def __del__(self):
        self.cursor.close()
        self.connection.close()


class MongoConnector:
    def __init__(self, mongodb_config):
        self.client = MongoClient(
            host=mongodb_config["host"], port=mongodb_config["port"])

    def use(self, db):
        self.db = self.client[db]
        return self

    def create(self, db):
        if self.db_exists(db):
            db += f"_{uuid4().hex[:8]}"
        return self.use(db)

    def db_exists(self, db):
        return db in self.client.list_database_names()

    def insert_many(self, collection_name, collection_values):
        self.db[collection_name].insert_many(collection_values)

    def apply_foreign_keys(self, foreign_keys):
        for (table_name, column_name, referenced_table_name, referenced_column_name) in foreign_keys:
            for doc in self.db[table_name].find():
                self.db[table_name].update({
                    column_name: doc[column_name]
                }, {
                    "$set": {
                        column_name: self.db[referenced_table_name].find_one(
                            {
                                referenced_column_name: doc[column_name]
                            })["_id"]
                    }
                })

        self.rename_columns_from_foreign_keys(foreign_keys)

    def rename_columns_from_foreign_keys(self, foreign_keys):
        for (table_name, column_name, referenced_table_name, *_rest) in foreign_keys:
            self.db[table_name].update_many({}, {
                "$rename": {
                    column_name: f"{referenced_table_name}_id"
                }
            })

    def remove_primary_keys(self, primary_keys):
        for (table_name, primary_key) in primary_keys:
            self.db[table_name].update_many({}, {
                "$unset": {
                    primary_key: 1
                }
            })

    def get_tables_names(self):
        return self.db.collection_names()

    def get_results(self, table_name):
        return self.db[table_name].find()

    def get_columns_infos(self, table_name):
        return self.db[table_name].find_one().keys()

    def get_collection(self, table_name):
        collection = []
        for result in self.get_results(table_name):
            document = {}
            for field in self.get_columns_infos(table_name):
                document[field] = result[field]
            collection.append(document)
        return collection

    def __del__(self):
        self.client.close()


class CassandraConnector:
    def __init__(self, cassandra_config):
        self.cluster = cluster.Cluster([
            cassandra_config["host"]
        ], port=cassandra_config["port"])
        self.session = self.cluster.connect()

    def use(self, keyspace):
        if self.keyspace_exists(keyspace):
            self.session.execute(f"USE {self.keyspace}")
            return self
        return None

    def create(self, keyspace):
        if self.keyspace_exists(keyspace):
            self.keyspace += f"_{uuid4().hex[:8]}"

        self.session.execute(
            f"CREATE KEYSPACE {self.keyspace} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1}}")
        return self.use(self.keyspace)

    def keyspace_exists(self, keyspace):
        self.keyspace = keyspace.replace("-", "_")
        keyspaces = self.session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces")
        return any([rs.keyspace_name == self.keyspace for rs in keyspaces])

    def get_table_types(self, document):
        return {key.replace("_", ""): type(document[key]) for key in document.keys()}

    def create_table(self, table_name, document):
        table_types = self.get_table_types(document)
        types = []
        for field_name in table_types.keys():
            field_type = table_types[field_name]
            if field_type == ObjectId:
                if field_name == "id":
                    types.append("id text PRIMARY KEY")
                else:
                    types.append(f"{field_name} text")
            elif field_type == str:
                types.append(f"{field_name} text")
            elif field_type == int:
                types.append(f"{field_name} int")
            elif field_type == float:
                types.append(f"{field_name} float")
            elif field_type == bool:
                types.append(f"{field_name} boolean")
            elif field_type == bytes:
                types.append(f"{field_name} blob")

        types_str = ", ".join(types)
        self.session.execute(f"CREATE TABLE {table_name} ({types_str})")

    def insert_many(self, collection_name, documents):
        self.create_table(collection_name, documents[0])
        for document in documents:
            altered_document = {}
            for key in document.keys():
                if type(document[key]) == ObjectId:
                    altered_document[key.replace("_", "")] = str(document[key])
                else:
                    altered_document[key.replace("_", "")] = document[key]
            print(altered_document)
            column_names = ", ".join(altered_document.keys())
            column_values = ", ".join(
                f"%({key})s" for key in altered_document.keys())

            self.session.execute(
                f"INSERT INTO {collection_name} ({column_names}) VALUES ({column_values})", altered_document)
