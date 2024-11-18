.PHONY: activate format_sql gen_profile install install_deps install_dev_deps install_python_deps lint lint_sql remove_lock

# Cross-platform settings
ifeq ($(OS),Windows_NT)
    RM = del /Q
    PYTHON = python
    PIP = pip
    SHELL_CMD = powershell
else
    RM = rm -f
    PYTHON = python3
    PIP = pip3
    SHELL_CMD = bash
endif

# Activate virtual environment
activate:
	@echo "Activating virtual environment"
	@poetry shell

# Generate the DBT profile using a Python script
gen_profile:
	@echo "Generating profiles.yml"
	@op inject -i profiles.yml.tpl -o profiles.yml

# Run complete installation
install: remove_lock install_python_deps gen_profile

# Install both development and project dependencies
install_deps: install_dev_deps activate gen_profile install_python_deps

# Install Poetry globally (cross-platform)
install_dev_deps:
	@echo "Installing Poetry globally"
	@$(PIP) install poetry --break-system-packages || $(PIP) install poetry

# Install Python libraries using Poetry
install_python_deps:
	@echo "Installing Python libraries"
	@poetry install --with dev --no-root

# Conditionally remove poetry.lock if it exists
remove_lock:
	@echo "Checking if poetry.lock exists and removing it if found"
	@if [ -f poetry.lock ]; then \
		echo "Removing poetry.lock"; \
		$(RM) poetry.lock; \
	else \
		echo "No poetry.lock found"; \
	fi
