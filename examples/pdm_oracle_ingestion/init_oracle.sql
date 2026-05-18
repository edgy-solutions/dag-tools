-- gvenzl/oracle-free runs *.sql files in /container-entrypoint-initdb.d/
-- as SYS connected to CDB$ROOT. Switch to the application user's PDB and
-- schema so the tables land where the pdm user can see them.
ALTER SESSION SET CONTAINER = FREEPDB1;

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

-- Stats table written to by the Restate handler, one row per ack batch.
CREATE TABLE pdm.pdm_stats (
    cycle_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rows_acked    NUMBER NOT NULL,
    source_table  VARCHAR2(64) NOT NULL,
    acked_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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

COMMIT;
