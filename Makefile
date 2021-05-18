SHELL := cmd

NAME := dbimport

ifneq ($(shell where python /q && echo 0),0)
  $(error Couldn't find 'python'. Make sure it is installed and available on PATH)
endif

WORK_DIR := $(shell python -c "import os,sys; print(os.path.normpath(os.path.dirname(' '.join(sys.argv[1:]).strip())))" "$(MAKEFILE_LIST)")

ENV_DIR := $(WORK_DIR)\.venv
ENV_BIN := $(ENV_DIR)\Scripts

ENV_MARKER := $(ENV_BIN)\activate.bat
REQS_MARKER := $(ENV_DIR)\requirements
REQS_DEV_MARKER := $(ENV_DIR)\requirements-dev

.PHONY: test
test: $(ENV_MARKER) $(REQS_MARKER)
	@$(ENV_BIN)\python -m unittest discover -s "$(WORK_DIR)"

.PHONY: run
run: $(ENV_MARKER) $(REQS_MARKER)
	@set "PYTHONPATH=$(WORK_DIR)" && $(ENV_BIN)\python -m "$(NAME)"

.PHONY: bundle
bundle: $(ENV_MARKER) $(REQS_MARKER)
	@$(ENV_BIN)\pyinstaller \
		--distpath "$(WORK_DIR)\dist" \
		--workpath "$(WORK_DIR)\build" \
		--noconfirm \
		--clean \
		--log-level INFO \
		"$(WORK_DIR)\pyinstaller.spec"

.PHONY: format
format: format-imports format-code

.PHONY: format-imports
format-imports: $(ENV_MARKER) $(REQS_DEV_MARKER)
	@$(ENV_BIN)\isort "$(WORK_DIR)"

.PHONY: format-code
format-code: $(ENV_MARKER) $(REQS_DEV_MARKER)
	@$(ENV_BIN)\black "$(WORK_DIR)"

.PHONY: env
env: $(ENV_MARKER)

$(ENV_MARKER):
	@python -m venv "$(ENV_DIR)" && $(ENV_BIN)\python -m pip install -U pip

.PHONY: install
install: $(ENV_MARKER) $(REQS_MARKER)

$(REQS_MARKER):
	@$(ENV_BIN)\python -m pip install \
		--requirement "$(WORK_DIR)\requirements-lock.txt" \
		--no-warn-script-location \
		&& \
		type nul > "$(REQS_MARKER)"

.PHONY: install-dev
install-dev: $(ENV_MARKER) $(REQS_DEV_MARKER)

$(REQS_DEV_MARKER):
	@$(ENV_BIN)\python -m pip install \
		--requirement "$(WORK_DIR)\requirements-dev.txt" \
		--no-warn-script-location \
		&& \
		type nul > "$(REQS_DEV_MARKER)"

.PHONY: clean-all
clean-all: clean clean-env clean-bundle

.PHONY: clean
clean:
	@for /d /r "$(WORK_DIR)" %%d in ("__pycache__") do @if exist "%%d" rd /s /q "%%d"

.PHONY: clean-env
clean-env:
	@if exist "$(ENV_DIR)" rd /s /q "$(ENV_DIR)"

.PHONY: clean-bundle
clean-bundle:
	@if exist "$(WORK_DIR)\build" rd /s /q "$(WORK_DIR)\build"
	@if exist "$(WORK_DIR)\dist" rd /s /q "$(WORK_DIR)\dist"
