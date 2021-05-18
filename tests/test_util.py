import unittest

from dbimport.util import (
    is_cast_explicit,
    qualify_name,
    quote_name,
    translate_dtype,
)


class TestUtil(unittest.TestCase):
    def test_qualify_name(self):
        self.assertEqual("Schema.Table", qualify_name("Schema", "Table"))
        self.assertEqual("Table", qualify_name("", "Table"))
        self.assertEqual("Table", qualify_name(None, "Table"))

    def test_translate_dtype(self):
        cases = {
            "object": "text",
            "str": "text",
            "string": "text",
            "int64": "number",
            "longlong": "number",
            "uint64": "number",
            "ulonglong": "number",
            "float64": "decimal",
            "datetime64[ns]": "datetime",
            "bool": "bool",
        }

        for name, exp in cases.items():
            self.assertEqual(exp, translate_dtype(name))

    def test_is_cast_explicit(self):
        cases = {
            ("text", "char"): False,
            ("text", "char(10)"): False,
            ("text", "nvarchar(10)"): False,
            ("text", "number"): True,
            ("number", "int"): False,
            ("number", "decimal"): False,
            ("number", "decimal(10)"): False,
            ("number", "decimal(10, 5)"): False,
            ("number", "numeric"): False,
            ("number", "numeric(10)"): False,
            ("number", "numeric(10, 5)"): False,
            ("number", "nvarchar(10)"): True,
            ("decimal", "float"): False,
            ("decimal", "decimal"): True,
            ("decimal", "decimal(10)"): True,
            ("decimal", "decimal(10, 0)"): True,
            ("decimal", "decimal(10, 1)"): False,
            ("decimal", "numeric"): True,
            ("decimal", "numeric(10)"): True,
            ("decimal", "numeric(10, 0)"): True,
            ("decimal", "numeric(10, 1)"): False,
            ("decimal", "nvarchar(10)"): True,
            ("datetime", "datetime"): False,
            ("datetime", "datetime2"): False,
            ("datetime", "datetimeoffset"): False,
            ("datetime", "smalldatetime"): False,
            ("datetime", "date"): True,
            ("datetime", "time"): True,
        }

        for (src, dst), exp in cases.items():
            self.assertIs(exp, is_cast_explicit(src, dst))

    def test_quote_name(self):
        cases = {
            "": "[]",
            "]": "[]]]",
            "[": "[[]",
            "][][": "[]][]][]",
            "a": "[a]",
            "[a]": "[[a]]]",
            "a" * 128: "[" + "a" * 128 + "]",
            "a" * 129: None,
        }

        for s, exp in cases.items():
            if exp is None or s is None:
                assert_func = self.assertIs
            else:
                assert_func = self.assertEqual

            assert_func(exp, quote_name(s))
