import os.path
from collections import OrderedDict

import pandas as pd
import pyodbc
from PySide2 import QtCore, QtGui, QtWidgets
from PySide2.QtCore import Qt
from PySide2.QtGui import QPalette
from PySide2.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDesktopWidget,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from .importer import Importer
from .util import (
    get_column_metadata,
    is_cast_explicit,
    message_box,
    qualify_name,
    translate_dtype,
)


class QLineEditClick(QLineEdit):
    clicked = QtCore.Signal()

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            # noinspection PyUnresolvedReferences
            self.clicked.emit()
        else:
            super().mousePressEvent(event)


class Window(QWidget):
    def __init__(self):
        # noinspection PyArgumentList
        super().__init__()

        self._dsns = OrderedDict()
        self._dsns_schema_table_map = OrderedDict()

        self._file = OrderedDict()

        self._cols_join_on = {}
        self._cols_subset = {}

        self._current_dsn = None

        self.setWindowTitle("Database Importer")
        # noinspection PyArgumentList
        self.resize(650, 700)

        edt_palette = QPalette()
        color = edt_palette.color(QPalette.Disabled, QPalette.Base)
        edt_palette.setColor(QPalette.Active, QPalette.Base, color)
        edt_palette.setColor(QPalette.Inactive, QPalette.Base, color)

        # noinspection PyArgumentList
        self.lbl_dsn = QLabel()
        self.lbl_dsn.setText("Data Source:")
        self.lbl_dsn.setFixedWidth(70)
        self.lbl_dsn.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        # noinspection PyArgumentList
        self.cmb_dsn = QComboBox()
        # noinspection PyUnresolvedReferences
        self.cmb_dsn.currentTextChanged.connect(self.populate_tables)

        # noinspection PyArgumentList
        self.lbl_tbl = QLabel()
        self.lbl_tbl.setText("Table:")
        self.lbl_tbl.setFixedWidth(70)
        self.lbl_tbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        # noinspection PyArgumentList
        self.cmb_tbl = QComboBox()
        # noinspection PyUnresolvedReferences
        self.cmb_tbl.currentTextChanged.connect(self.update_table_attributes)

        # noinspection PyArgumentList
        self.lbl_file = QLabel()

        self.lbl_file.setText("File:")
        self.lbl_file.setFixedWidth(70)
        self.lbl_file.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        # noinspection PyArgumentList
        self.edt_file = QLineEditClick()
        self.edt_file.setPlaceholderText("Browse...")
        self.edt_file.setReadOnly(True)
        self.edt_file.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
        self.edt_file.setPalette(edt_palette)
        # noinspection PyUnresolvedReferences
        self.edt_file.clicked.connect(self.browse_file)

        # noinspection PyArgumentList
        self.lbl_sht = QLabel()
        self.lbl_sht.setText("Sheet:")
        self.lbl_sht.setFixedWidth(70)
        self.lbl_sht.setAlignment(Qt.AlignRight | Qt.AlignVCenter)

        # noinspection PyArgumentList
        self.cmb_sht = QComboBox()
        # noinspection PyUnresolvedReferences
        self.cmb_sht.currentTextChanged.connect(
            self.update_file_details_clear_content
        )

        self.tbl_cols = QTableWidget(0, 5, self)
        self.tbl_cols.setHorizontalHeaderLabels(
            [
                "Table Column Name",
                "Table Data Type",
                "File Column Name",
                "File Data Type",
                "Join",
            ]
        )
        self.tbl_cols.horizontalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.ResizeToContents
        )
        self.tbl_cols.verticalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.Fixed
        )
        self.tbl_cols.setEditTriggers(
            QtWidgets.QAbstractItemView.NoEditTriggers
        )
        self.tbl_cols.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)

        # noinspection PyArgumentList
        self.btn_update = QPushButton()
        self.btn_update.setText("Update")
        # noinspection PyUnresolvedReferences
        self.btn_update.clicked.connect(self.import_data)

        layout_dsn = QHBoxLayout()
        layout_dsn.addWidget(self.lbl_dsn, stretch=0)
        layout_dsn.addWidget(self.cmb_dsn, stretch=1)

        layout_tbl = QHBoxLayout()
        layout_tbl.addWidget(self.lbl_tbl, stretch=0)
        layout_tbl.addWidget(self.cmb_tbl, stretch=1)

        layout_file = QHBoxLayout()
        layout_file.addWidget(self.lbl_file, stretch=0)
        layout_file.addWidget(self.edt_file, stretch=1)

        layout_sheet = QHBoxLayout()
        layout_sheet.addWidget(self.lbl_sht, stretch=0)
        layout_sheet.addWidget(self.cmb_sht, stretch=1)

        layout_columns = QVBoxLayout()
        layout_columns.addWidget(self.tbl_cols, stretch=1)

        layout_download = QHBoxLayout()
        layout_download.addStretch()
        layout_download.addWidget(self.btn_update)
        layout_download.addStretch()

        layout_main = QVBoxLayout()
        layout_main.addLayout(layout_dsn)
        layout_main.addLayout(layout_tbl)
        layout_main.addLayout(layout_file)
        layout_main.addLayout(layout_sheet)
        layout_main.addLayout(layout_columns)
        layout_main.addLayout(layout_download)
        self.setLayout(layout_main)

        self.disable_all()
        self.populate_dsn_cmb()
        self.center()

    def center(self):
        frame_geometry = self.frameGeometry()
        # noinspection PyArgumentList
        center = QDesktopWidget().availableGeometry().center()
        frame_geometry.moveCenter(center)
        self.move(frame_geometry.topLeft())

    def disable_all(self):
        self.cmb_dsn.setEnabled(False)
        self.cmb_tbl.setEnabled(False)
        self.edt_file.setEnabled(False)
        self.cmb_sht.setEnabled(False)
        self.tbl_cols.setEnabled(False)
        self.btn_update.setEnabled(False)

    def populate_dsn_cmb(self):
        data_sources = sorted(pyodbc.dataSources())
        if data_sources:
            self.cmb_dsn.blockSignals(True)
            self.cmb_dsn.addItems(data_sources)
            self.cmb_dsn.setCurrentIndex(-1)
            self.cmb_dsn.setEnabled(True)
            self.cmb_dsn.blockSignals(False)
        else:
            self.disable_all()

    def _update_dsns(self, name, columns):
        self._dsns[name] = columns
        self._dsns_schema_table_map[name] = OrderedDict()

        for pair in columns:
            self._dsns_schema_table_map[name][qualify_name(*pair)] = pair

    def _get_schema_table_pair(self, dsn, table):
        if not dsn:
            raise ValueError("invalid data source '%s'" % dsn)
        if not table:
            raise ValueError("invalid table '%s'" % table)
        return self._dsns_schema_table_map[dsn][table]

    def _get_columns(self, dsn, table):
        return self._dsns[dsn][self._get_schema_table_pair(dsn, table)]

    def populate_tables(self):
        if self.cmb_dsn.currentIndex() == -1:
            return

        dsn = self.cmb_dsn.currentText()
        if dsn not in self._dsns:
            try:
                connection = pyodbc.connect("DSN=%s;" % dsn)
                cursor = connection.cursor()

                self._update_dsns(dsn, get_column_metadata(cursor))

            except pyodbc.Error as e:
                if not self._current_dsn:
                    self.cmb_dsn.setCurrentIndex(-1)
                else:
                    self.cmb_dsn.setCurrentIndex(
                        self.cmb_dsn.findText(self._current_dsn)
                    )
                message_box(
                    e.args[1] if len(e.args) > 1 else e,
                    parent=self,
                    exit_app=False,
                )
                return
            else:
                self._current_dsn = dsn

        tables = self._dsns_schema_table_map[dsn]

        if tables:
            self.cmb_tbl.clear()
            self.cmb_tbl.addItems(tables.keys())
            self.cmb_tbl.setEnabled(True)
            self.edt_file.setEnabled(True)
            self.tbl_cols.setEnabled(True)
        else:
            self.cmb_tbl.setEnabled(False)
            self.edt_file.setEnabled(False)
            self.tbl_cols.setEnabled(False)

    def populate_sheet(self):
        sheets = self._file.keys()
        if sheets:
            self.cmb_sht.clear()
            self.cmb_sht.addItems(list(sheets))
            self.cmb_sht.setEnabled(True)
        else:
            self.cmb_sht.setEnabled(False)

    def browse_file(self):
        fp, _ = QFileDialog.getOpenFileName(
            self, "Open", "", "Excel Workbook (*.xlsx)"
        )

        if fp:
            if self.load_file(fp):
                self.edt_file.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                self.edt_file.setText(os.path.normpath(fp))

                self.populate_sheet()

        if not self.edt_file.text():
            self.edt_file.setAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
            self.edt_file.clearFocus()

    def load_file(self, fp):
        try:
            spreadsheet = pd.read_excel(fp, sheet_name=None, dtype=object)
        except Exception as e:
            message_box(e, parent=self, exit_app=False)
            return False
        else:
            self._file.clear()

            for sheet, data in spreadsheet.items():
                data = data.convert_dtypes()

                columns = []
                for col, dtype in data.dtypes.iteritems():
                    columns.append((col, translate_dtype(dtype.name)))

                self._file[sheet] = (OrderedDict(columns), data)
            return True

    def update_table_attributes(self):
        dsn = self.cmb_dsn.currentText()
        table = self.cmb_tbl.currentText()
        if not dsn or not table:
            return

        columns = self._get_columns(dsn, table)

        self.tbl_cols.setRowCount(len(columns))
        for i, (table_col, table_col_type) in enumerate(columns.items()):
            # noinspection PyArgumentList
            file_col_widget = QWidget()
            # noinspection PyArgumentList
            file_col_cmb = QComboBox()
            file_col_cmb.setMinimumWidth(100)
            # noinspection PyUnresolvedReferences
            file_col_cmb.activated.connect(
                self.update_file_details_keep_content
            )
            # noinspection PyArgumentList
            file_col_cmb_layout = QHBoxLayout(file_col_widget)
            file_col_cmb_layout.addWidget(file_col_cmb)
            file_col_cmb_layout.setAlignment(Qt.AlignCenter)
            file_col_cmb_layout.setContentsMargins(10, 0, 10, 0)

            # noinspection PyArgumentList
            file_join_widget = QWidget()
            # noinspection PyArgumentList
            file_join_ckb = QCheckBox()
            file_join_ckb.setEnabled(False)
            file_join_ckb.setVisible(False)
            # noinspection PyUnresolvedReferences
            file_join_ckb.clicked.connect(
                self.update_file_details_keep_content
            )
            # noinspection PyArgumentList
            file_join_ckb_layout = QHBoxLayout(file_join_widget)
            file_join_ckb_layout.addWidget(file_join_ckb)
            file_join_ckb_layout.setAlignment(Qt.AlignCenter)
            file_join_ckb_layout.setContentsMargins(0, 0, 0, 0)

            # noinspection PyArgumentList
            self.tbl_cols.setItem(i, 0, QTableWidgetItem(table_col))
            # noinspection PyArgumentList
            self.tbl_cols.setItem(i, 1, QTableWidgetItem(table_col_type))
            self.tbl_cols.setCellWidget(i, 2, file_col_widget)
            self.tbl_cols.setCellWidget(i, 4, file_join_widget)

        self.update_file_details_clear_content()

    def update_file_details(self, keep_content):
        sheet = self.cmb_sht.currentText()
        if not sheet:
            return

        sheet_data = self._file.get(sheet)
        if not sheet_data:
            return

        rows_num = self.tbl_cols.rowCount()
        columns, _ = sheet_data

        available = list(columns)
        if keep_content:
            for i in range(rows_num):
                file_col_cmb = self.tbl_cols.cellWidget(i, 2).findChild(
                    QComboBox
                )
                # noinspection PyUnresolvedReferences
                col = file_col_cmb.currentText()
                if col and col in available:
                    available.remove(col)

        self._cols_join_on.clear()
        self._cols_subset.clear()

        default = [""] + available
        for i in range(rows_num):
            table_col = self.tbl_cols.item(i, 0).text()
            file_col_cmb = self.tbl_cols.cellWidget(i, 2).findChild(QComboBox)
            file_join_ckb = self.tbl_cols.cellWidget(i, 4).findChild(QCheckBox)

            # noinspection PyUnresolvedReferences
            selected = file_col_cmb.currentText()
            if selected and keep_content:
                table_col_type = self.tbl_cols.item(i, 1).text()

                # noinspection PyUnresolvedReferences
                file_col_cmb.clear()
                # noinspection PyUnresolvedReferences
                file_col_cmb.addItems(
                    [""]
                    + [c for c in columns if c in available or c == selected]
                )
                # noinspection PyUnresolvedReferences
                file_col_cmb.setCurrentIndex(file_col_cmb.findText(selected))

                file_col_type = columns[selected]
                # noinspection PyArgumentList
                file_col_type_item = QTableWidgetItem(file_col_type)
                if is_cast_explicit(file_col_type, table_col_type):
                    file_col_type_item.setBackground(Qt.yellow)
                self.tbl_cols.setItem(i, 3, file_col_type_item)

                # noinspection PyUnresolvedReferences
                file_join_ckb.setEnabled(True)
                # noinspection PyUnresolvedReferences
                file_join_ckb.setVisible(True)

                # noinspection PyUnresolvedReferences
                if file_join_ckb.isChecked():
                    self._cols_join_on[selected] = table_col
                else:
                    self._cols_subset[selected] = table_col
            else:
                # noinspection PyUnresolvedReferences
                file_col_cmb.clear()
                # noinspection PyUnresolvedReferences
                file_col_cmb.addItems(default)

                self.tbl_cols.takeItem(i, 3)

                # noinspection PyUnresolvedReferences
                file_join_ckb.setChecked(False)
                # noinspection PyUnresolvedReferences
                file_join_ckb.setEnabled(False)
                # noinspection PyUnresolvedReferences
                file_join_ckb.setVisible(False)

        if len(self._cols_join_on) > 0 and len(self._cols_subset) > 0:
            self.btn_update.setEnabled(True)
        else:
            self.btn_update.setEnabled(False)

    def update_file_details_keep_content(self):
        self.update_file_details(keep_content=True)

    def update_file_details_clear_content(self):
        self.update_file_details(keep_content=False)

    def import_data(self):
        # noinspection PyArgumentList
        QApplication.setOverrideCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))

        dsn = self.cmb_dsn.currentText()
        table_qualified = self.cmb_tbl.currentText()
        schema, table = self._get_schema_table_pair(dsn, table_qualified)
        sheet = self.cmb_sht.currentText()
        columns, data = self._file[sheet]

        data = data.copy()[
            list(self._cols_join_on) + list(self._cols_subset)
        ].rename(columns={**self._cols_join_on, **self._cols_subset})

        conn = pyodbc.connect("DSN=%s;" % dsn)
        try:
            importer = Importer(
                connection=conn,
                data=data,
                table=table,
                schema=schema,
                join_on=list(self._cols_join_on.values()),
                subset=list(self._cols_subset.values()),
            )
            importer.run(update=True)
        except Exception as e:
            if isinstance(e, pyodbc.Error):
                e = e.args[1] if len(e.args) > 1 else e
            # noinspection PyArgumentList
            QApplication.restoreOverrideCursor()

            message_box(e, parent=self, exit_app=False)
        else:
            rows = importer.row_count_updated

            if rows < 0:
                msg = "Updated unknown number of rows"
            elif rows == 0:
                msg = "No rows were updated"
            elif rows == 1:
                msg = "Successfully updated %d row" % rows
            else:
                msg = "Successfully updated %d rows" % rows

            # noinspection PyArgumentList
            QApplication.restoreOverrideCursor()

            message_box(msg, parent=self, error=False, exit_app=False)
        finally:
            conn.close()
