/* SPDX-License-Identifier: Apache-2.0 */
/* lance_tam 0.1 (docs/design.md §2.4, §2.6, §2.9, §2.10) */

\echo Use "CREATE EXTENSION lance_tam" to load this file. \quit

-- The table access method
CREATE FUNCTION lance.tableam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'lance_tableam_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD lance TYPE TABLE HANDLER lance.tableam_handler;
COMMENT ON ACCESS METHOD lance IS 'Lance datasets, local or on object storage, as tables';

-- Lance's own indices of a table's dataset, as PostgreSQL indexes
CREATE FUNCTION lance.indexam_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'lance_indexam_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD lance_index TYPE INDEX HANDLER lance.indexam_handler;
COMMENT ON ACCESS METHOD lance_index IS 'an index of a lance table''s dataset, Lance''s own';

CREATE OPERATOR CLASS lance.lance_ops DEFAULT FOR TYPE anyelement USING lance_index AS
    STORAGE anyelement;

-- The catalog: a row a lance table (§2.6)
CREATE TABLE lance.tables (
    relid oid NOT NULL PRIMARY KEY,
    uri text NOT NULL,
    version bigint NOT NULL,
    managed boolean NOT NULL,
    writable boolean NOT NULL,
    follow boolean NOT NULL DEFAULT false,
    columns text[] NOT NULL,
    options text[],
    server text
);
REVOKE ALL ON lance.tables FROM PUBLIC;
COMMENT ON TABLE lance.tables IS 'each lance table''s dataset, and the version of it the table is';

-- Row ids: each value the first of a chunk of 65536 (lance_tam.h)
CREATE SEQUENCE lance.rowid_seq
    START WITH 549755813888 INCREMENT BY 65536
    MINVALUE 549755813888 MAXVALUE 1099511562240 NO CYCLE;
REVOKE ALL ON SEQUENCE lance.rowid_seq FROM PUBLIC;

-- Storage servers: credentials in user mappings, out of the tables' options
CREATE FUNCTION lance.storage_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'lance_storage_validator'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER lance_storage VALIDATOR lance.storage_validator;
COMMENT ON FOREIGN DATA WRAPPER lance_storage IS 'object stores lance tables keep their datasets in';

-- What a table's dataset holds
CREATE FUNCTION lance.table_info(rel regclass)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'lance_table_info'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION lance.schema(rel regclass)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'lance_schema'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION lance.versions_json(rel regclass)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'lance_versions'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION lance.versions(rel regclass)
RETURNS TABLE (version bigint, "timestamp" timestamptz, metadata jsonb)
LANGUAGE sql STRICT VOLATILE
AS $$
    SELECT (v->>'version')::bigint, (v->>'timestamp')::timestamptz, v->'metadata'
    FROM jsonb_array_elements(lance.versions_json(rel)) AS v
    ORDER BY 1
$$;

CREATE FUNCTION lance.indices(rel regclass)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'lance_indices'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION lance.fragments(rel regclass)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'lance_fragments'
LANGUAGE C STRICT STABLE;

CREATE FUNCTION lance.explain(rel regclass, filter text DEFAULT NULL, verbose boolean DEFAULT false)
RETURNS text
AS 'MODULE_PATHNAME', 'lance_explain'
LANGUAGE C STABLE;

-- Maintenance
CREATE FUNCTION lance.optimize(rel regclass, options jsonb DEFAULT '{}')
RETURNS jsonb
AS 'MODULE_PATHNAME', 'lance_optimize'
LANGUAGE C VOLATILE;

CREATE FUNCTION lance.cleanup(rel regclass, older_than interval DEFAULT '1 hour')
RETURNS jsonb
AS 'MODULE_PATHNAME', 'lance_cleanup'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION lance.refresh(rel regclass)
RETURNS bigint
AS 'MODULE_PATHNAME', 'lance_refresh'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION lance.engine_version()
RETURNS text
AS 'MODULE_PATHNAME', 'lance_engine_version'
LANGUAGE C STRICT STABLE;
