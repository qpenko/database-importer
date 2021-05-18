import sqlite3
import unittest

import pandas as pd

from dbimport.importer import Importer, ImporterError


class TestImporter(unittest.TestCase):
    schema = """create table groceries (
        id text not null primary key,
        item text,
        quantity int,
        price real
        );

    insert into groceries values ('ID000001', 'Apple', 5, 10.0);
    insert into groceries values ('ID000002', 'Pear', 4, 9.0);
    insert into groceries values ('ID000003', 'Orange', 3, 8.0);
    insert into groceries values ('ID000004', 'Lemon', 6, 7.0);
    """

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")

        if self.shortDescription() == "schema_no_pk":
            schema = self.schema.replace(
                "id text not null primary key", "id text not null"
            )
        elif self.shortDescription() == "schema_number_pk":
            schema = (
                self.schema.replace(
                    "id text not null primary key",
                    "number int primary key, id text not null",
                )
                .replace("'ID000001'", "1, 'ID000001'")
                .replace("'ID000002'", "2, 'ID000002'")
                .replace("'ID000003'", "3, 'ID000003'")
                .replace("'ID000004'", "4, 'ID000004'")
            )
        else:
            schema = self.schema

        cur = self.conn.cursor()
        cur.executescript(schema)

        self.conn.commit()

        cur.close()

    def tearDown(self):
        self.conn.close()

    def fetchall(self, table, query="select * from {table}", *args):
        cur = self.conn.cursor()
        yield from cur.execute(query.format(table=table), *args).fetchall()
        cur.close()

    def test_setup(self):
        exp = [
            ("ID000001", "Apple", 5, 10.0),
            ("ID000002", "Pear", 4, 9.0),
            ("ID000003", "Orange", 3, 8.0),
            ("ID000004", "Lemon", 6, 7.0),
        ]
        act = list(self.fetchall("groceries"))

        self.assertEqual(exp, act)

    def test_init(self):
        df = pd.DataFrame(
            [
                ("ID000001", "Apple", 15, 20.0),
                ("ID000002", "Pear", 14, 19.0),
                ("ID000003", "Orange", 13, 18.0),
                ("ID000004", "Lemon", 16, 17.0),
            ],
            columns=["id", "item", "quantity", "price"],
        )
        table = "groceries"
        dialect = "sqlite"

        imp = Importer(
            connection=self.conn, data=df, table=table, dialect=dialect
        )

        exp_data = df.to_dict()
        exp_table = table
        exp_dialect = dialect
        exp_join_on = ["id"]
        exp_subset = ["item", "quantity", "price"]
        exp_table_pk = ["id"]
        exp_table_cols = ["id", "item", "quantity", "price"]

        act_data = imp._data_master.to_dict()
        act_table = imp._table
        act_dialect = imp._dialect
        act_join_on = imp._join_on
        act_join_on_getter = imp.join_on
        act_subset = imp._subset
        act_subset_getter = imp.subset
        act_table_pk = imp._table_pk
        act_table_pk_getter = imp.table_primary_key
        act_table_cols = imp._table_cols
        act_table_cols_getter = imp.table_columns

        self.assertEqual(exp_data, act_data)
        self.assertEqual(exp_table, act_table)
        self.assertEqual(exp_dialect, act_dialect)
        self.assertEqual(exp_join_on, act_join_on)
        self.assertEqual(exp_join_on, act_join_on_getter)
        self.assertEqual(exp_subset, act_subset)
        self.assertEqual(exp_subset, act_subset_getter)
        self.assertEqual(exp_table_pk, act_table_pk)
        self.assertEqual(exp_table_pk, act_table_pk_getter)
        self.assertEqual(exp_table_cols, act_table_cols)
        self.assertEqual(exp_table_cols, act_table_cols_getter)

    def test_init_empty(self):
        df = pd.DataFrame([], columns=["id", "item", "quantity", "price"])

        with self.assertRaisesRegex(ValueError, "data contains no records"):
            Importer(
                connection=self.conn,
                data=df,
                table="groceries",
                dialect="sqlite",
            )

    def test_unknown_dialect(self):
        df = pd.DataFrame(
            [("ID000001", "Apple", 15, 20.0)],
            columns=["id", "item", "quantity", "price"],
        )

        with self.assertRaisesRegex(
            ValueError, "unsupported dialect, use available: .*"
        ):
            Importer(
                connection=self.conn,
                data=df,
                table="groceries",
                dialect="mysql",
            )

    def test_update(self):
        values = [
            ("ID000001", "Apple", 15, 20.0),
            ("ID000002", "Pear", 14, 19.0),
            ("ID000003", "Orange", 13, 18.0),
            ("ID000004", "Lemon", 16, 17.0),
        ]

        df = pd.DataFrame(values, columns=["id", "item", "quantity", "price"])

        imp = Importer(
            connection=self.conn, data=df, table="groceries", dialect="sqlite"
        )
        imp.run(update=True)

        exp = values
        act = list(self.fetchall("groceries"))

        self.assertEqual(exp, act)

    def test_join_on_column_contains_nulls(self):
        values = [
            ("ID000001", "Apple", 15, 20.0),
            (None, "Pear", 14, 19.0),
            ("ID000003", "Orange", 13, 18.0),
            (None, "Lemon", 16, 17.0),
        ]

        df = pd.DataFrame(values, columns=["id", "item", "quantity", "price"])

        imp = Importer(
            connection=self.conn, data=df, table="groceries", dialect="sqlite"
        )
        imp.run(update=True)

        exp = [
            ("ID000001", "Apple", 15, 20.0),
            ("ID000002", "Pear", 4, 9.0),
            ("ID000003", "Orange", 13, 18.0),
            ("ID000004", "Lemon", 6, 7.0),
        ]
        act = list(self.fetchall("groceries"))

        self.assertEqual(exp, act)

    def test_join_on_non_key_column(self):
        """schema_number_pk"""
        df = pd.DataFrame(
            [("ID000001", "Apple", 15, 20.0)],
            columns=["id", "item", "quantity", "price"],
        )

        Importer(
            connection=self.conn,
            data=df,
            table="groceries",
            join_on=["id"],
            dialect="sqlite",
        )

        with self.assertRaisesRegex(
            ValueError, r"column\(s\) to join on are required"
        ):
            Importer(
                connection=self.conn,
                data=df,
                table="groceries",
                dialect="sqlite",
            )

    def test_join_on_column_not_supplied(self):
        """schema_no_pk"""
        df = pd.DataFrame(
            [("ID000001", "Apple", 15, 20.0)],
            columns=["id", "item", "quantity", "price"],
        )

        with self.assertRaisesRegex(
            ValueError, r"column\(s\) to join on are required"
        ):
            Importer(
                connection=self.conn,
                data=df,
                table="groceries",
                dialect="sqlite",
            )

    def test_join_on_column_missing(self):
        df = pd.DataFrame(
            [("ID000001", "Apple", 15, 20.0)],
            columns=["id", "item", "quantity", "price"],
        )

        with self.assertRaisesRegex(
            ValueError, "couldn't find supplied column to join on: 'index'"
        ):
            Importer(
                connection=self.conn,
                data=df,
                table="groceries",
                join_on=["index"],
                dialect="sqlite",
            )

    def test_subset_invalid_column(self):
        df = pd.DataFrame(
            [("ID000001", "Apple", 15, 20.0)],
            columns=["id", "item", "quantity", "price"],
        )

        with self.assertRaisesRegex(
            ValueError, "column provided not found in data: 'size'"
        ):
            Importer(
                connection=self.conn,
                data=df,
                table="groceries",
                subset=["id", "item", "size"],
                dialect="sqlite",
            )

    def test_subset_invalid_table_column(self):
        df = pd.DataFrame(
            [("ID000001", "Apple", 15, 20.0, 1)],
            columns=["id", "item", "quantity", "price", "size"],
        )

        with self.assertRaisesRegex(
            ValueError,
            "column provided not found in 'groceries' table: 'size'",
        ):
            Importer(
                connection=self.conn,
                data=df,
                table="groceries",
                subset=["id", "item", "size"],
                dialect="sqlite",
            )

    def test_slice_data_duplicate_columns(self):
        values = [
            ("ID000001", "Apple", 15, 20.0, 10.0),
            ("ID000002", "Pear", 14, 19.0, 9.0),
            ("ID000003", "Orange", 13, 18.0, 8.0),
            ("ID000004", "Lemon", 16, 17.0, 7.0),
        ]

        df = pd.DataFrame(
            values, columns=["id", "item", "quantity", "price", "price"]
        )

        with self.assertRaisesRegex(
            ImporterError, "data contains duplicate column: 'price'"
        ):
            Importer(
                connection=self.conn,
                data=df,
                table="groceries",
                dialect="sqlite",
            )

    def test_slice_data_duplicate_values(self):
        values = [
            ("ID000001", "Apple", 15, 20.0),
            ("ID000002", "Pear", 14, 19.0),
            ("ID000002", "Orange", 13, 18.0),
            ("ID000004", "Lemon", 16, 17.0),
        ]

        df = pd.DataFrame(values, columns=["id", "item", "quantity", "price"])

        with self.assertRaisesRegex(
            ImporterError,
            "data contains duplicate values in join on column: 'id'",
        ):
            Importer(
                connection=self.conn,
                data=df,
                table="groceries",
                dialect="sqlite",
            )
