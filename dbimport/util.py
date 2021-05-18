import re
from collections import OrderedDict, defaultdict, namedtuple

from PySide2.QtCore import QCoreApplication
from PySide2.QtWidgets import QMessageBox


def message_box(text, parent=None, error=True, exit_app=True):
    """Show message box with 'OK' button."""
    text = str(text)
    msg_box = QMessageBox(parent=parent)

    if error:
        msg_box.setIcon(QMessageBox.Critical)
        msg_box.setWindowTitle("Error")
    else:
        msg_box.setIcon(QMessageBox.Information)
        msg_box.setWindowTitle("Information")

    msg = text[:1].upper() + text[1:]
    if not msg.endswith(".") and not msg.endswith("!"):
        msg += "."

    msg_box.setText(msg.ljust(30))
    msg_box.exec_()

    if exit_app:
        QCoreApplication.exit(error)


def get_column_metadata(cursor):
    """Return details of each column that can be accessed in the current
    database."""
    inf_schema_query = """SELECT TABLE_SCHEMA
        , TABLE_NAME
        , COLUMN_NAME
        , DATA_TYPE
        , COALESCE(
              CHARACTER_MAXIMUM_LENGTH
            , NUMERIC_PRECISION
            , DATETIME_PRECISION
        ) AS COLUMN_SIZE
        , NUMERIC_SCALE
    FROM INFORMATION_SCHEMA.COLUMNS
    ORDER BY TABLE_SCHEMA
        , TABLE_NAME
        , ORDINAL_POSITION
    """

    inf_schema_cols = namedtuple(
        "InformationSchemaColumns",
        [
            "table_schema",
            "table_name",
            "column_name",
            "type_name",
            "column_size",
            "decimal_digits",
        ],
    )
    columns = defaultdict(OrderedDict)

    for r in cursor.execute(inf_schema_query):
        row = inf_schema_cols(*r)

        if "char" in row.type_name:
            details = "(%d)" % row.column_size
        elif row.type_name in ("decimal", "numeric"):
            details = "(%d, %d)" % (row.column_size, row.decimal_digits)
        else:
            details = ""

        value = row.type_name + details
        columns[(row.table_schema, row.table_name)][row.column_name] = value

    return columns


def qualify_name(schema, table):
    """Return qualified table name from a pair of schema, table values."""
    if schema:
        return schema + "." + table
    else:
        return table


def translate_dtype(name):
    """Return common data type name instead of pandas' / numpy's type name."""
    name_l = name.lower()
    if name_l == "object" or name_l.startswith("str"):
        return "text"
    elif (
        name_l.startswith("int")
        or name_l.startswith("uint")
        or name_l.startswith("longlong")
        or name_l.startswith("ulonglong")
    ):
        return "number"
    elif name_l.startswith("float"):
        return "decimal"
    elif name_l.startswith("datetime"):
        return "datetime"
    else:
        return name


def is_cast_explicit(src, dst):
    """Return False if source (spreadsheet) data type can be implicitly
    converted into the destination (database) data type.

    Return True otherwise.
    """
    if src == "text":
        if "char" in dst or "text" in dst:
            return False
    elif src == "number":
        if (
            dst == "int"
            or dst.startswith("decimal")
            or dst.startswith("numeric")
        ):
            return False
    elif src == "decimal":
        if dst == "float":
            return False
        m = re.match(r"^(decimal|numeric)\((\d+)\s*,\s*(\d+)\)$", dst)
        if m and int(m.group(3)) > 0:
            return False
    elif src == "datetime":
        if "datetime" in dst:
            return False
    return True


def quote_name(s):
    """Return a string with the delimiters (brackets) added to make the input
    string a valid SQL Server delimited identifier.

    Return None if input string is greater than 128 characters.
    """
    if len(s) > 128:
        return None
    return "[" + s.replace("]", "]]") + "]"
