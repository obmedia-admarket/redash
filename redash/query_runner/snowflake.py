import os
import tempfile
import logging
import json
import re

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

try:
    import snowflake.connector

    enabled = True
except ImportError:
    enabled = False


logger = logging.getLogger(__name__)

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)


from redash import __version__
from redash.query_runner import (
    TYPE_BOOLEAN,
    TYPE_DATE,
    TYPE_DATETIME,
    TYPE_FLOAT,
    TYPE_INTEGER,
    TYPE_STRING,
    BaseSQLQueryRunner,
    register,
)

TYPES_MAP = {
    0: TYPE_INTEGER,
    1: TYPE_FLOAT,
    2: TYPE_STRING,
    3: TYPE_DATE,
    4: TYPE_DATETIME,
    5: TYPE_STRING,
    6: TYPE_DATETIME,
    7: TYPE_DATETIME,
    8: TYPE_DATETIME,
    13: TYPE_BOOLEAN,
}


class Snowflake(BaseSQLQueryRunner):
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "account": {"type": "string"},
                "user": {"type": "string"},
                "password": {"type": "string"},
                "warehouse": {"type": "string"},
                "database": {"type": "string"},
                "region": {"type": "string", "default": "us-west"},
                "lower_case_columns": {
                    "type": "boolean",
                    "title": "Lower Case Column Names in Results",
                    "default": False,
                },
                "host": {"type": "string"},
            },
            "order": [
                "account",
                "user",
                "password",
                "warehouse",
                "database",
                "region",
                "host",
            ],
            "required": ["user", "password", "account", "database", "warehouse"],
            "secret": ["password"],
            "extra_options": [
                "host",
            ],
        }

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def determine_type(cls, data_type, scale):
        t = TYPES_MAP.get(data_type, None)
        if t == TYPE_INTEGER and scale > 0:
            return TYPE_FLOAT
        return t

    def _get_connection(self):
        region = self.configuration.get("region")
        account = self.configuration["account"]

        # for us-west we don't need to pass a region (and if we do, it fails to connect)
        if region == "us-west":
            region = None

        if self.configuration.__contains__("host"):
            host = self.configuration.get("host")
        else:
            if region:
                host = "{}.{}.snowflakecomputing.com".format(account, region)
            else:
                host = "{}.snowflakecomputing.com".format(account)

        connection_parameters = {
            "user": self.configuration["user"],
            "password": self.configuration["password"],
            "account": account,
            "region": region,
            "host": host,
            "application": "Redash/{} (Snowflake)".format(__version__.split("-")[0]),
            "session_parameters": {
                "QUERY_TAG": "redash",
            }
        }

        private_key_content = os.getenv("SNOWFLAKE_PRIVATE_KEY_CONTENT")
        private_key_password = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSWORD")

        if private_key_content and private_key_password:

            p_key = serialization.load_pem_private_key(
                private_key_content.encode(),
                password=private_key_password.encode(),
                backend=default_backend()
            )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            connection_parameters['private_key'] = pkb

            # password is not needed
            connection_parameters.pop('password')
            logging.debug("Connectiong with the private key from SNOWFLAKE_PRIVATE_KEY_CONTENT and SNOWFLAKE_PRIVATE_KEY_PASSWORD env vars")


        return snowflake.connector.connect(**connection_parameters)

    def _column_name(self, column_name):
        if self.configuration.get("lower_case_columns", False):
            return column_name.lower()

        return column_name

    def _parse_results(self, cursor):
        columns = self.fetch_columns(
            [(self._column_name(i[0]), self.determine_type(i[1], i[5])) for i in cursor.description]
        )
        rows = [dict(zip((column["name"] for column in columns), row)) for row in cursor]

        data = {"columns": columns, "rows": rows}
        return data

    def run_query(self, query, user):
        connection = self._get_connection()
        cursor = connection.cursor()

        try:
            cursor.execute("USE WAREHOUSE {}".format(self.configuration["warehouse"]))
            cursor.execute("USE {}".format(self.configuration["database"]))

            cursor.execute(query)

            data = self._parse_results(cursor)
            error = None
        finally:
            cursor.close()
            connection.close()

        return data, error

    def _run_query_without_warehouse(self, query):
        connection = self._get_connection()
        cursor = connection.cursor()

        try:
            cursor.execute("USE {}".format(self.configuration["database"]))

            cursor.execute(query)

            data = self._parse_results(cursor)
            error = None
        finally:
            cursor.close()
            connection.close()

        return data, error

    def _database_name_includes_schema(self):
        return "." in self.configuration.get("database")

    def get_schema(self, get_stats=False):
        if self._database_name_includes_schema():
            query = "SHOW COLUMNS"
        else:
            query = "SHOW COLUMNS IN DATABASE"

        results, error = self._run_query_without_warehouse(query)

        if error is not None:
            self._handle_run_query_error(error)

        schema = {}
        for row in results["rows"]:
            if row["kind"] == "COLUMN":
                table_name = "{}.{}".format(row["schema_name"], row["table_name"])

                if table_name not in schema:
                    schema[table_name] = {"name": table_name, "columns": []}

                schema[table_name]["columns"].append(row["column_name"])

        return list(schema.values())


register(Snowflake)
