.PHONY: build

SHELL := $(shell type bash | cut -d\  -f3)
PROJECT_DIR := $(shell realpath .)

VIRTUAL_ENV ?= $(shell poetry env info -p 2>/dev/null || find . -type d -name '*v*env' -exec realpath {} \;)
PYTHON_VERSION := $(shell cat .python-version 2>/dev/null || python3 -V | sed "s,.* \(3\.[0-9]\+\)\..*,\1,")


env-info:
	@echo -e """\
	Source files: ${PROJECT_DIR}/${SRC_DIR}\n\
	Virtual env: ${VIRTUAL_ENV}\n\
	Current Python: ${PYTHON_VERSION}\n\
	""" | sed "s,: ,:|,;s,^\t,," | column -t -s\|
	@[ -f "${VIRTUAL_ENV}/bin/activate" ] && echo "* Run \`source ${VIRTUAL_ENV}/bin/activate\` to activate the venv *"

env-setup:
	@python -m venv virtualenv
	source virtualenv/bin/activate && python -m pip install -r requirements.txt
