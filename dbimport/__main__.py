import sys

from PySide2.QtWidgets import QApplication

from dbimport.util import message_box
from dbimport.window import Window


def exception_hook(exc_type, value, traceback):
    sys.__excepthook__(exc_type, value, traceback)
    message_box(value)


def gui_main(argv):
    ec = 1
    try:
        sys.excepthook = exception_hook

        app = QApplication(argv)
        window = Window()
        window.show()

        ec = app.exec_()
    except Exception as e:
        message_box(e)
    return ec


sys.exit(gui_main(sys.argv))
