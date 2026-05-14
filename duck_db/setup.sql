-- Network Intrusion Detection DuckDB Schema

CREATE SEQUENCE IF NOT EXISTS seq_traffic START 1;
CREATE SEQUENCE IF NOT EXISTS seq_summary START 1;

CREATE TABLE IF NOT EXISTS network_traffic (
    id                  INTEGER DEFAULT nextval('seq_traffic'),
    dst_port            INTEGER,
    duration            DOUBLE,
    src_pkts            INTEGER,
    flow_bytes_per_sec  DOUBLE,
    flow_pkts_per_sec   DOUBLE,
    mean_pkt_len        DOUBLE,
    std_pkt_len         DOUBLE,
    fin_flag            INTEGER,
    psh_flag            INTEGER,
    ack_flag            INTEGER,
    attack_category     VARCHAR,
    is_attack           INTEGER,
    source_dataset      VARCHAR
);

CREATE TABLE IF NOT EXISTS attack_summary (
    id                      INTEGER DEFAULT nextval('seq_summary'),
    source_dataset          VARCHAR,
    attack_category         VARCHAR,
    is_attack               INTEGER,
    record_count            BIGINT,
    avg_duration            DOUBLE,
    avg_flow_bytes_per_sec  DOUBLE,
    avg_flow_pkts_per_sec   DOUBLE,
    avg_mean_pkt_len        DOUBLE,
    avg_src_pkts            DOUBLE
);

CREATE OR REPLACE VIEW v_attack_distribution AS
SELECT
    source_dataset,
    attack_category,
    is_attack,
    SUM(record_count)               AS total_records,
    AVG(avg_duration)               AS avg_duration,
    AVG(avg_flow_bytes_per_sec)     AS avg_flow_bytes_per_sec,
    AVG(avg_flow_pkts_per_sec)      AS avg_flow_pkts_per_sec,
    AVG(avg_mean_pkt_len)           AS avg_mean_pkt_len
FROM attack_summary
GROUP BY source_dataset, attack_category, is_attack;

CREATE OR REPLACE VIEW v_dataset_overview AS
SELECT
    source_dataset,
    COUNT(*)                                        AS total_records,
    SUM(CASE WHEN is_attack = 1 THEN 1 ELSE 0 END) AS attack_records,
    SUM(CASE WHEN is_attack = 0 THEN 1 ELSE 0 END) AS benign_records,
    ROUND(AVG(duration), 4)                         AS avg_duration,
    ROUND(AVG(flow_bytes_per_sec), 2)               AS avg_flow_bytes_per_sec
FROM network_traffic
GROUP BY source_dataset;

CREATE OR REPLACE VIEW v_top_attack_categories AS
SELECT
    attack_category,
    source_dataset,
    SUM(record_count)   AS total_records,
    AVG(avg_duration)   AS avg_duration
FROM attack_summary
WHERE is_attack = 1
GROUP BY attack_category, source_dataset
ORDER BY total_records DESC;
