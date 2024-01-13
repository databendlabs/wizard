# Makefile

# Python command
PYTHON = python3

# Python formatter
FORMATTER = black

# Source directory for Python files (current directory and all subdirectories)
SRC_DIR = .

# Target for installing the formatter
install-formatter:
	$(PYTHON) -m pip install $(FORMATTER)

# Target for formatting all Python files
lint:
	$(FORMATTER) $(SRC_DIR)

# Other targets can go here

.PHONY: install-formatter format

