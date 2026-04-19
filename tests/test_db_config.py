import importlib
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


class FakeOracleDb(types.SimpleNamespace):
    class ProgrammingError(Exception):
        pass

    def __init__(self):
        super().__init__()
        self.init_calls = []
        self.makedsn_calls = []
        self.connect_calls = []

    def init_oracle_client(self, *, lib_dir):
        self.init_calls.append(lib_dir)

    def makedsn(self, host, port, *, service_name):
        self.makedsn_calls.append((host, port, service_name))
        return f"dsn://{host}:{port}/{service_name}"

    def connect(self, *, user, password, dsn):
        self.connect_calls.append((user, password, dsn))
        return {"user": user, "password": password, "dsn": dsn}


class ConnectDbConfigTests(unittest.TestCase):
    @staticmethod
    def _import_target(module_name: str, fake_oracle: FakeOracleDb):
        sys.modules.pop(module_name, None)
        fake_numpy = types.SimpleNamespace()
        fake_pandas = types.SimpleNamespace(DataFrame=object)
        with mock.patch.dict(
            sys.modules,
            {
                "oracledb": fake_oracle,
                "numpy": fake_numpy,
                "pandas": fake_pandas,
            },
        ):
            return importlib.import_module(module_name)

    def test_connect_db_reads_env_credentials(self):
        env = {
            "DAS_DB_HOST": "db.example.com",
            "DAS_DB_PORT": "1521",
            "DAS_DB_SID": "xe",
            "DAS_DB_USER": "scott",
            "DAS_DB_PASSWORD": "tiger",
        }
        fake_oracle = FakeOracleDb()
        module = self._import_target("Das_Clustering_repair", fake_oracle)
        with mock.patch.dict(os.environ, env, clear=True):
            conn = module.connect_db(client_path="/tmp/instantclient")

        self.assertEqual(fake_oracle.init_calls, ["/tmp/instantclient"])
        self.assertEqual(fake_oracle.makedsn_calls, [("db.example.com", 1521, "xe")])
        self.assertEqual(
            fake_oracle.connect_calls,
            [("scott", "tiger", "dsn://db.example.com:1521/xe")],
        )
        self.assertEqual(conn["user"], "scott")

    def test_connect_db_accepts_json_config_file(self):
        payload = {
            "host": "db.internal",
            "port": 3306,
            "sid": "TMS30",
            "user": "msfs",
            "password": "secret",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "db_config.json"
            config_path.write_text(json.dumps(payload), encoding="utf-8")

            fake_oracle = FakeOracleDb()
            module = self._import_target("Das_Clustering_repair", fake_oracle)
            with mock.patch.dict(os.environ, {}, clear=True):
                conn = module.connect_db(
                    db_config_path=str(config_path),
                    client_path="/tmp/instantclient",
                )

            self.assertEqual(fake_oracle.makedsn_calls, [("db.internal", 3306, "TMS30")])
            self.assertEqual(
                fake_oracle.connect_calls,
                [("msfs", "secret", "dsn://db.internal:3306/TMS30")],
            )
            self.assertEqual(conn["dsn"], "dsn://db.internal:3306/TMS30")

    def test_connect_db_raises_clear_error_without_credentials(self):
        fake_oracle = FakeOracleDb()
        module = self._import_target("Das_Clustering_repair", fake_oracle)
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError) as exc_info:
                module.connect_db(client_path="/tmp/instantclient")

        message = str(exc_info.exception)
        self.assertIn("Missing DB connection settings", message)
        self.assertIn("DAS_DB_HOST", message)
        self.assertIn("DAS_DB_PASSWORD", message)


if __name__ == "__main__":
    unittest.main()
