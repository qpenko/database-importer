import os

NAME = "dbimport"

a = Analysis(
    [os.path.join(SPECPATH, NAME, "__main__.py")],
    pathex=[],
    binaries=None,
    datas=[],
    hiddenimports=[],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
)

pyz = PYZ(
    a.pure,
    a.zipped_data,
    cipher=None,
)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name=NAME,
    debug=False,
    strip=False,
    upx=False,
    console=False,
    icon="NONE",
)
