# Test bed and end-to-end scenarios (deploy/, e2e/). Included from the root Makefile.
# Commands assume the repository root is the cwd.
#
#   make bed-up         start the bed (NBHA_IMPL=auto: HA build when cmd/*/Dockerfile exist)
#   make bed-down       remove the bed (containers, volumes, network)
#   make e2e            fresh bed with the HA build (2 instances), every scenario, bed-down
#   make e2e-baseline   the same with stock upstream images (1 instance unless NBHA_INSTANCES=2)
#   make e2e-run        the scenarios against the bed that is up now
#
# Variables: NBHA_INSTANCES=1|2, NBHA_LB_MODE=l4|l7, NBHA_SIGNAL_BALANCE=first|roundrobin,
# KEEP_BED=1 (leave the bed up after e2e), E2E_RUN=<go test -run regexp>,
# NBHA_E2E_LONG=1 (ephemeral and soak scenarios), NBHA_SOAK=24h.

E2E_RUN ?= .
E2E_TIMEOUT ?= 90m
E2E_STATE := $(CURDIR)/deploy/.state

.PHONY: bed-up bed-down bed-status e2e e2e-baseline e2e-run

bed-up:
	deploy/bed.sh up

bed-down:
	deploy/bed.sh down

bed-status:
	deploy/bed.sh status

e2e-run: SHELL := /bin/bash
e2e-run:
	mkdir -p $(E2E_STATE)
	cd e2e && NBHA_E2E=1 NBHA_E2E_REPORT=$(E2E_STATE)/e2e-report.tsv \
		go test -count=1 -v -timeout $(E2E_TIMEOUT) -run '$(E2E_RUN)' . 2>&1 | tee $(E2E_STATE)/e2e.log; \
		exit $${PIPESTATUS[0]}

e2e: SHELL := /bin/bash
e2e:
	deploy/bed.sh down >/dev/null 2>&1 || true
	NBHA_IMPL=$${NBHA_IMPL:-ha} deploy/bed.sh up
	rc=0; $(MAKE) --no-print-directory e2e-run || rc=$$?; \
		[ "$(KEEP_BED)" = 1 ] || deploy/bed.sh down; exit $$rc

e2e-baseline: SHELL := /bin/bash
e2e-baseline:
	deploy/bed.sh down >/dev/null 2>&1 || true
	NBHA_IMPL=stock deploy/bed.sh up
	rc=0; $(MAKE) --no-print-directory e2e-run || rc=$$?; \
		[ "$(KEEP_BED)" = 1 ] || deploy/bed.sh down; exit $$rc
