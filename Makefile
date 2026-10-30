# SPDX-License-Identifier: Apache-2.0
# lance_tam (docs/design.md §2.1): the extension, built by PGXS.  The engine
# library is built apart, by test/engine.sh in the builder image, and
# installed beside the module as liblance_engine.so.

MODULE_big = lance_tam
OBJS = \
	src/catalog.o \
	src/convert.o \
	src/customscan.o \
	src/encoding.o \
	src/engine.o \
	src/filter.o \
	src/functions.o \
	src/indexam.o \
	src/module.o \
	src/options.o \
	src/rowids.o \
	src/scan.o \
	src/tableam.o \
	src/vexec.o \
	src/xact.o

EXTENSION = lance_tam
DATA = sql/lance_tam--0.1.sql
PG_CPPFLAGS = -I$(srcdir)/include -I$(srcdir)/src
SHLIB_LINK = -ldl

REGRESS_OPTS = --inputdir=test/regress --outputdir=$(LT_REGRESS_OUT)

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# The engine library, from the builder's output (test/run.sh engine)
LT_ENGINE ?= /engine/liblance_engine.so
install-engine:
	$(INSTALL_SHLIB) $(LT_ENGINE) '$(DESTDIR)$(pkglibdir)/liblance_engine.so'
.PHONY: install-engine
