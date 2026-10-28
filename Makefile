# Run from the root of this repo.
#
# register-json: register every data-model JSON schema in the Synapse schema
# registry under the MC2Center organization, without binding it to an entity.
# This WRITES to Synapse; preview with `make -n register-json ...` first.
#
#   make register-json SCHEMAS=../data-models/json_schemas VERSION=14.0.0
#
# VERSION  Release number, with or without a leading "v" (default: 14.0.0).
#          synapse_json_schema_bind.py wants "-v v<number>" and splits on "v".
# SCHEMAS  Path to the data-models `json_schemas` directory, checked out at the
#          tag that matches VERSION (default: ../data-models/json_schemas).
#
# Each schema is its own target, so make stops at the first failed registration
# (do not pass -k) and `make -n` lists one command per schema.

VERSION ?= 14.0.0
SCHEMAS ?= ../data-models/json_schemas
ORG_NAME ?= MC2Center
PYTHON ?= python

SCHEMA_VERSION := $(patsubst v%,%,$(VERSION))
SCHEMA_FILES := $(sort $(wildcard $(SCHEMAS)/*.json))
REGISTER_TARGETS := $(addprefix register-json/,$(notdir $(SCHEMA_FILES)))

.PHONY: help register-json $(REGISTER_TARGETS)

# A bare `make` must never write to Synapse.
.DEFAULT_GOAL := help

help:
	@echo "Targets:"
	@echo "  register-json  Register every JSON schema in SCHEMAS under ORG_NAME at VERSION (writes to Synapse)."
	@echo "                 Preview first: make -n register-json SCHEMAS=../data-models/json_schemas"

ifneq ($(filter register-json,$(MAKECMDGOALS)),)
ifeq ($(SCHEMA_FILES),)
$(error No *.json files found in SCHEMAS=$(SCHEMAS); set SCHEMAS to the data-models json_schemas directory)
endif
endif

register-json: $(REGISTER_TARGETS)

$(REGISTER_TARGETS): register-json/%:
	$(PYTHON) utils/synapse_json_schema_bind.py -p "$(SCHEMAS)/$*" -n $(ORG_NAME) -v v$(SCHEMA_VERSION) --no_bind
