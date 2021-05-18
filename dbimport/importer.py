from typing import List, Optional

import numpy as np
import pandas as pd

from .util import quote_name as q


class ImporterError(Exception):
    pass


class Importer:
    _chunk_size = 5000
    _known_dialects = {"mssql", "sqlite"}
    _temp_table = "dbimport"

    _query_get_pk = {
        "mssql": """select column_name
        from information_schema.key_column_usage
        where table_schema = ?
            and table_name = ?
        order by ordinal_position""",
        "sqlite": """select name
        from pragma_table_info(?)
        where pk = 1
        order by cid""",
    }

    _query_get_cols = {
        "mssql": """select column_name
        from information_schema.columns
        where table_schema = ?
            and table_name = ?
        order by ordinal_position""",
        "sqlite": """select name
        from pragma_table_info(?)
        order by cid""",
    }

    _query_drop_temp_table = {
        "mssql": """if object_id('tempdb.dbo.{temp}') is not null
        drop table {temp}""",
        "sqlite": """drop table if exists temp.{temp}""",
    }

    _query_create_temp_table = {
        "mssql": """select top 0 {cols} into {temp} from {table}""",
        "sqlite": """create temp table {temp} as
        select {cols} from {table} limit 0""",
    }

    def __init__(
        self,
        connection,
        data: pd.DataFrame,
        table: str,
        schema: Optional[str] = None,
        join_on: Optional[List[str]] = None,
        subset: Optional[List[str]] = None,
        dialect="mssql",
    ):
        if data.empty:
            raise ValueError("data contains no records")

        if dialect not in self._known_dialects:
            raise ValueError(
                "unsupported dialect, use available: %s"
                % ", ".join("'%s'" % c for c in sorted(self._known_dialects))
            )

        if (
            not isinstance(data.index, (pd.MultiIndex, pd.RangeIndex))
            and data.index.name
        ):
            data = data.reset_index()

        self._conn = connection
        self._data = None
        self._data_master = data
        self._table = table
        self._schema = schema
        self._dialect = dialect
        self._row_cnt_upd = -1
        self._row_cnt_ins = -1

        if dialect == "mssql":
            if self._schema is None:
                self._schema = "dbo"
            self._temp_table = "#" + self._temp_table

        self._join_on: List[str] = []
        self._subset: List[str] = []

        cur = self._conn.cursor()
        self._table_pk = self._get_pk(cur)
        self._table_cols = self._get_cols(cur)
        cur.close()

        join_cols = join_on or [c for c in data if c in self._table_pk]
        subset_cols = [c for c in subset or data if c not in join_cols]

        self._set_join_on(join_cols)
        self._set_subset(subset_cols)
        self._slice_data()

    @property
    def join_on(self) -> List[str]:
        return self._join_on

    @join_on.setter
    def join_on(self, columns: List[str]) -> None:
        self._set_join_on(columns)
        self._slice_data()

    @property
    def subset(self) -> List[str]:
        return self._subset

    @subset.setter
    def subset(self, columns: List[str]) -> None:
        self._set_subset(columns)
        self._slice_data()

    @property
    def table_primary_key(self) -> List[str]:
        return self._table_pk

    @property
    def table_columns(self) -> List[str]:
        return self._table_cols

    @property
    def row_count_updated(self):
        return self._row_cnt_upd

    @property
    def row_count_inserted(self):
        return self._row_cnt_ins

    @staticmethod
    def _unique(values: List[str]) -> List[str]:
        unique: List[str] = []
        for value in values:
            if value not in unique:
                unique.append(value)
        return unique

    def _get_pk(self, cur) -> List[str]:
        query = self._query_get_pk[self._dialect]
        if self._dialect == "mssql":
            params = (self._schema, self._table)
        else:  # sqlite
            params = (self._table,)
        return [row for row, in cur.execute(query, params).fetchall()]

    def _get_cols(self, cur) -> List[str]:
        query = self._query_get_cols[self._dialect]
        if self._dialect == "mssql":
            params = (self._schema, self._table)
        else:  # sqlite
            params = (self._table,)
        return [row for row, in cur.execute(query, params).fetchall()]

    def _set_join_on(self, columns: List[str]) -> None:
        if not columns:
            raise ValueError("column(s) to join on are required")

        columns = self._unique(columns)

        diff = set(columns) - set(self._data_master.columns)
        if diff:
            raise ValueError(
                "couldn't find supplied column%s to join on: %s"
                % (
                    "s" if len(diff) > 1 else "",
                    ", ".join("'%s'" % c for c in sorted(diff)),
                )
            )

        self._join_on[:] = columns

    def _set_subset(self, columns: List[str]) -> None:
        if not columns:
            raise ValueError("no columns provided")

        columns = self._unique(columns)

        diff = set(columns) - set(self._data_master.columns)
        if diff:
            raise ValueError(
                "column%s provided not found in data: %s"
                % (
                    "s" if len(diff) > 1 else "",
                    ", ".join("'%s'" % c for c in sorted(diff)),
                )
            )

        diff = set(columns) & set(self._join_on)
        if diff:
            raise ValueError(
                "column%s provided cannot contain join on column%s: %s"
                % (
                    "s" if len(columns) > 1 else "",
                    "s" if len(diff) > 1 else "",
                    ", ".join("'%s'" % c for c in sorted(diff)),
                )
            )

        diff = set(columns) - set(self._table_cols)
        if diff:
            if self._schema is not None:
                table = self._schema + "." + self._table
            else:
                table = self._table

            raise ValueError(
                "column%s provided not found in '%s' table: %s"
                % (
                    "s" if len(diff) > 1 else "",
                    table,
                    ", ".join("'%s'" % c for c in sorted(diff)),
                )
            )

        self._subset[:] = columns

    def _slice_data(self):
        cols = self._join_on + self._subset
        data = self._data_master[cols].copy().dropna(subset=self._join_on)

        if data.columns.has_duplicates:
            duplicates = data.columns[data.columns.duplicated()]
            raise ImporterError(
                "data contains duplicate column%s: %s"
                % (
                    "s" if len(duplicates) > 1 else "",
                    ", ".join("'%s'" % c for c in duplicates),
                )
            )

        if data.duplicated(self._join_on).any():
            raise ImporterError(
                "data contains duplicate values in join on column%s: %s"
                % (
                    "s" if len(self._join_on) > 1 else "",
                    ", ".join("'%s'" % c for c in self._join_on),
                )
            )

        self._data = data

    def _executemany(self, cur, query: str, data: pd.DataFrame) -> None:
        for _, chunk in data.groupby(np.arange(len(data)) // self._chunk_size):
            cur.executemany(
                query,
                chunk.astype(object)
                .where(pd.notnull(chunk), None)
                .values.tolist(),
            )
            self._conn.commit()

    def _drop_temp_table(self, cur):
        drop_temp = self._query_drop_temp_table[self._dialect]
        drop_temp_query = drop_temp.format(temp=self._temp_table)
        cur.execute(drop_temp_query)

    def _fill_temp_table(self, cur):
        create_temp = self._query_create_temp_table[self._dialect]
        insert_temp = "insert into {temp} ({cols}) values ({vals})"

        if self._dialect == "mssql":
            table = q(self._schema) + "." + q(self._table)
            cols = ", ".join(q(col) for col in self._data.columns)
        else:  # sqlite
            table = self._table
            cols = ", ".join(self._data.columns)

        create_temp_query = create_temp.format(
            temp=self._temp_table,
            table=table,
            cols=cols,
        )
        insert_temp_query = insert_temp.format(
            temp=self._temp_table,
            cols=cols,
            vals=", ".join("?" for _ in range(len(self._data.columns))),
        )

        cur.execute(create_temp_query)
        self._executemany(cur, insert_temp_query, self._data)

    def run(self, update=True, insert=False):
        if not update and not insert:
            raise ValueError("at least one action must be performed")

        cur = self._conn.cursor()
        if self._dialect == "mssql" and hasattr(cur, "fast_executemany"):
            cur.fast_executemany = True

        self._drop_temp_table(cur)
        self._fill_temp_table(cur)

        if update:
            self._update(cur)
        if insert:
            self._insert(cur)

        self._drop_temp_table(cur)
        cur.close()

    def _update(self, cur) -> None:
        if self._dialect == "mssql":
            condition = " and ".join(
                "a.{col} = b.{col}".format(col=q(col)) for col in self._join_on
            )
            cols = ", ".join(
                "a.{col} = b.{col}".format(col=q(col)) for col in self._subset
            )

            query = """update a
            set {cols}
            from {table} as a
            inner join {temp} as b
            on {cond}""".format(
                cols=cols,
                table=q(self._schema) + "." + q(self._table),
                temp=self._temp_table,
                cond=condition,
            )
        else:  # sqlite
            condition = " and ".join(
                "{table}.{col} = {temp}.{col}".format(
                    col=col,
                    table=self._table,
                    temp=self._temp_table,
                )
                for col in self._join_on
            )
            cols = ",\n".join(
                "{col} = (select {col} from {temp} where {cond})".format(
                    col=col, temp=self._temp_table, cond=condition
                )
                for col in self._subset
            )

            query = """update {table}
            set {cols}
            where exists (select * from {temp} where {cond})""".format(
                cols=cols,
                table=self._table,
                temp=self._temp_table,
                cond=condition,
            )

        cur.execute(query)
        self._conn.commit()

        self._row_cnt_upd = cur.rowcount

    def _insert(self, cur) -> None:
        raise NotImplementedError
