# Makefile to build the project 

PROJECT_NAME = GDPR_Obfuscator
PYTHON_INTERPRETER = python
WD = $(shell pwd)
PYTHONPATH = $(WD)
SHELL := /bin/bash
PIP := pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python version"
	@(${PYTHON_INTERPRETER} --version)
	@echo ">>> Setting up VirtualEnv."
	@(${PYTHON_INTERPRETER} -m venv venv)

ACTIVATE_ENV := source venv/bin/activate

define execute_in_env
	@$(ACTIVATE_ENV) && export PYTHONPATH=${PYTHONPATH} && $1
endef

## Bulid the environment requirements
requirements: create-environment
	@$(call execute_in_env, $(PIP) install -r ./requirements.txt)

# Set Up
## Install bandit
bandit: create-environment
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety: create-environment
	$(call execute_in_env, $(PIP) install safety)

## Install flake8
flake8: create-environment
	$(call execute_in_env $(PIP) install flake8)

# Install black
#black: create-environment
#	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage: create-environment
	$(call execute_in_env, $(PIP) install coverage)



# Build/ Run
## Run the security test (bandit + safety)
security-test: bandit safety
	$(call execute_in_env, bandit -lll */*.py)
	$(call execute_in_env, safety check -r requirements.txt)

format-check: flake8
# $(call execute_in_env, black */*.py)
	$(call execute_in_env, flake8 */*.py)

unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest test/* -vvvrp --testdox)

check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run --omit 'venv/*' \
	-m pytest test/* && coverage report -m)

run-checks: security-test format-check unit-test check-coverage