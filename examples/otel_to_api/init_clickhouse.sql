-- Minimal stand-in for the OpenTelemetry ClickHouse exporter's trace
-- table, plus a couple of execution groups worth of sample spans.
--
-- The column types matter: attributes are Map(String, String), which is
-- why every value the mapping reads needs explicit coercion.
--
-- NOTE: comments may not appear *between* tuples of a VALUES list —
-- ClickHouse's ValuesBlockInputFormat parses the rest of the query as
-- data and fails with CANNOT_PARSE_INPUT_ASSERTION_FAILED. Hence one
-- INSERT per annotated group below rather than one big list.

CREATE DATABASE IF NOT EXISTS otel;

CREATE TABLE IF NOT EXISTS otel.otel_traces
(
    Timestamp          DateTime64(9),
    TraceId            String,
    SpanId             String,
    ParentSpanId       String,
    SpanName           LowCardinality(String),
    SpanKind           LowCardinality(String),
    ServiceName        LowCardinality(String),
    ResourceAttributes Map(LowCardinality(String), String),
    SpanAttributes     Map(LowCardinality(String), String),
    Duration           UInt64,
    StatusCode         LowCardinality(String),
    StatusMessage      String
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, Timestamp);

-- Execution group run-42, entity_1: two artifacts on one process item.
INSERT INTO otel.otel_traces
(Timestamp, TraceId, SpanId, ParentSpanId, SpanName, SpanKind, ServiceName,
 ResourceAttributes, SpanAttributes, Duration, StatusCode, StatusMessage)
VALUES
(now64(9) - INTERVAL 30 MINUTE, 'trace-42', 'span-1', '', 'execution.event', 'SPAN_KIND_INTERNAL', 'runner',
 {'service.name':'runner'},
 {'execution.group_id':'run-42','execution.terminal':'false','execution.outcome':'SUCCESS',
  'entity.id':'entity_1','entity.ids':'entity_1','item.name':'item_1','artifact.path':'file1.txt',
  'ci.branch':'main','ci.commit':'abc1234','metric.NUM_ERROR':'0','metric.TOTAL_TOGGLES':'81.2'},
 1000, 'STATUS_CODE_OK', ''),
(now64(9) - INTERVAL 29 MINUTE, 'trace-42', 'span-2', '', 'execution.event', 'SPAN_KIND_INTERNAL', 'runner',
 {'service.name':'runner'},
 {'execution.group_id':'run-42','execution.terminal':'false','execution.outcome':'SUCCESS',
  'entity.id':'entity_1','entity.ids':'entity_1','item.name':'item_1','artifact.path':'file2.txt',
  'ci.branch':'main','ci.commit':'abc1234','metric.NUM_ERROR':'0','metric.TOTAL_TOGGLES':'12.5'},
 1200, 'STATUS_CODE_OK', '');

-- entity_2 is unknown to the target API: its PATCH 404s and the aggregate
-- fallback creates it -- and ONLY it. This span also carries the terminal
-- marker that releases group run-42 from the readiness gate.
-- The item name deliberately contains spaces, an ampersand and a quote:
-- path segments must be percent-encoded or this call misroutes. It does
-- NOT contain a slash -- see the README on why an identifier with a
-- literal '/' cannot travel in a path segment at all.
INSERT INTO otel.otel_traces
(Timestamp, TraceId, SpanId, ParentSpanId, SpanName, SpanKind, ServiceName,
 ResourceAttributes, SpanAttributes, Duration, StatusCode, StatusMessage)
VALUES
(now64(9) - INTERVAL 28 MINUTE, 'trace-42', 'span-3', '', 'execution.event', 'SPAN_KIND_INTERNAL', 'runner',
 {'service.name':'runner'},
 {'execution.group_id':'run-42','execution.terminal':'true','execution.outcome':'FAILURE',
  'entity.id':'entity_2','entity.ids':'entity_2','item.name':'Login flow \'smoke\' & retry',
  'artifact.path':'file3.txt',
  'ci.branch':'main','ci.commit':'abc1234','metric.NUM_ERROR':'3','metric.TOTAL_TOGGLES':'44'},
 900, 'STATUS_CODE_ERROR', 'assertion failed');

-- Execution group run-99 has no terminal marker and is recent, so the
-- readiness gate holds it back until it goes quiet.
INSERT INTO otel.otel_traces
(Timestamp, TraceId, SpanId, ParentSpanId, SpanName, SpanKind, ServiceName,
 ResourceAttributes, SpanAttributes, Duration, StatusCode, StatusMessage)
VALUES
(now64(9), 'trace-99', 'span-9', '', 'execution.event', 'SPAN_KIND_INTERNAL', 'runner',
 {'service.name':'runner'},
 {'execution.group_id':'run-99','execution.terminal':'false','execution.outcome':'SUCCESS',
  'entity.id':'entity_9','entity.ids':'entity_9','item.name':'item_9','artifact.path':'file9.txt',
  'ci.branch':'develop','ci.commit':'def5678','metric.NUM_ERROR':'0'},
 800, 'STATUS_CODE_OK', '');
