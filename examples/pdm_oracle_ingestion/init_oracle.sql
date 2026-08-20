-- gvenzl/oracle-free runs *.sql files in /container-entrypoint-initdb.d/
-- as SYS connected to CDB$ROOT. Switch to the application user's PDB and
-- schema so the tables land where the pdm user can see them.
ALTER SESSION SET CONTAINER = FREEPDB1;

-- ---------------------------------------------------------------------------
-- The request side: what we ask PDM for.
-- ---------------------------------------------------------------------------

-- Top-level Major End Items. We write this table; PDM reads it, explodes
-- each MEI, and fills the staging tables below. Writing it is what starts
-- a transaction -- nothing else on our side asks PDM for anything.
CREATE TABLE pdm.pdm_mei_request (
    mei_number    VARCHAR2(64) NOT NULL,
    requested_by  VARCHAR2(64),
    requested_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- The control table, written by BOTH sides. PDM appends STARTED, then
-- COMPLETED tagged FULL or DELTA. We append CONSUMED once the data has
-- landed on our side.
--
-- COMPLETED is the only trustworthy signal that a load is whole: counting
-- rows in staging cannot distinguish "PDM finished" from "PDM is a third
-- of the way through committing", so a count-driven cycle would extract a
-- partial load and acknowledge it.
CREATE TABLE pdm.pdm_control (
    load_status   VARCHAR2(32) NOT NULL,
    load_type     VARCHAR2(16),
    load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX pdm.pdm_control_status_ix ON pdm.pdm_control (load_status, load_ts);

-- ---------------------------------------------------------------------------
-- The data side: what PDM fills for us.
-- ---------------------------------------------------------------------------
-- A real deployment has around a dozen of these. Three is enough to show
-- that each carries its OWN index and cursor -- the component takes them
-- per table rather than one key for the whole pipeline.
--
-- Only some are MEI-scoped. PDM_STAGING populates regardless; PDM_BOM and
-- PDM_ROUTING stay empty until the MEI table is written. Nothing in the
-- config distinguishes them, which is deliberate: the distinction lives
-- in PDM and would only rot if mirrored here.

CREATE TABLE pdm.pdm_staging (
    part_id           NUMBER PRIMARY KEY,
    part_number       VARCHAR2(64) NOT NULL,
    description       VARCHAR2(255),
    unit_of_measure   VARCHAR2(16),
    last_modified     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_flag    CHAR(1) DEFAULT 'N' CHECK (processed_flag IN ('N','Y')),
    sync_date         TIMESTAMP
);

CREATE INDEX pdm.pdm_staging_unprocessed_ix
    ON pdm.pdm_staging (processed_flag);

CREATE TABLE pdm.pdm_bom (
    bom_id            NUMBER PRIMARY KEY,
    mei_number        VARCHAR2(64),
    parent_part_id    NUMBER,
    child_part_id     NUMBER,
    quantity          NUMBER,
    last_modified     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_flag    CHAR(1) DEFAULT 'N' CHECK (processed_flag IN ('N','Y')),
    sync_date         TIMESTAMP
);

CREATE INDEX pdm.pdm_bom_unprocessed_ix ON pdm.pdm_bom (processed_flag);

CREATE TABLE pdm.pdm_routing (
    route_id          NUMBER PRIMARY KEY,
    mei_number        VARCHAR2(64),
    part_id           NUMBER,
    operation_seq     NUMBER,
    work_center       VARCHAR2(32),
    last_modified     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_flag    CHAR(1) DEFAULT 'N' CHECK (processed_flag IN ('N','Y')),
    sync_date         TIMESTAMP
);

CREATE INDEX pdm.pdm_routing_unprocessed_ix ON pdm.pdm_routing (processed_flag);

-- Stats table written to by the Restate ack handler, one row per batch.
CREATE TABLE pdm.pdm_stats (
    cycle_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rows_acked    NUMBER NOT NULL,
    source_table  VARCHAR2(64) NOT NULL,
    acked_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- Seed: stands in for a PDM full load that has already completed.
-- ---------------------------------------------------------------------------
-- PDM_STAGING is not MEI-scoped, so it has rows from the start. The
-- COMPLETED row is what lets the cycle sensor fire on first boot; without
-- it the sensor correctly waits, which is the whole point.

INSERT INTO pdm.pdm_staging (part_id, part_number, description, unit_of_measure)
VALUES (1, 'P-0001', 'Hex bolt M10x40',     'EA');
INSERT INTO pdm.pdm_staging (part_id, part_number, description, unit_of_measure)
VALUES (2, 'P-0002', 'Hex nut M10',         'EA');
INSERT INTO pdm.pdm_staging (part_id, part_number, description, unit_of_measure)
VALUES (3, 'P-0003', 'Washer M10',          'EA');
INSERT INTO pdm.pdm_staging (part_id, part_number, description, unit_of_measure)
VALUES (4, 'P-0004', 'Bearing 6204-2RS',    'EA');
INSERT INTO pdm.pdm_staging (part_id, part_number, description, unit_of_measure)
VALUES (5, 'P-0005', 'Lubricant grease',    'KG');

INSERT INTO pdm.pdm_control (load_status, load_type)
VALUES ('STARTED', 'FULL');
INSERT INTO pdm.pdm_control (load_status, load_type)
VALUES ('COMPLETED', 'FULL');

COMMIT;
