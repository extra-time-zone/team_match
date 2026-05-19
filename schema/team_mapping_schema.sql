CREATE DATABASE IF NOT EXISTS `team_mapping`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE `team_mapping`;

CREATE TABLE IF NOT EXISTS pipeline_run (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    run_key VARCHAR(120) NOT NULL,
    sport VARCHAR(32) NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    sources VARCHAR(255) NOT NULL,
    status ENUM('running', 'completed', 'failed') NOT NULL DEFAULT 'running',
    params JSON NULL,
    summary JSON NULL,
    error_message TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_pipeline_run_key (run_key)
);

CREATE TABLE IF NOT EXISTS our_team (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    sport VARCHAR(32) NOT NULL,
    canonical_name VARCHAR(200) NOT NULL,
    normalized_name VARCHAR(200) NOT NULL,
    country_code VARCHAR(20) NULL,
    gender VARCHAR(20) NULL,
    age_group VARCHAR(40) NULL,
    team_level VARCHAR(40) NULL,
    status ENUM('seed_candidate', 'confirmed', 'needs_review', 'rejected', 'inactive') NOT NULL DEFAULT 'seed_candidate',
    confidence DECIMAL(8,4) NOT NULL DEFAULT 0,
    confirmed_method VARCHAR(80) NULL,
    confirmed_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_our_team_sport_name (sport, normalized_name),
    KEY idx_our_team_status (status, sport, confidence)
);

CREATE TABLE IF NOT EXISTS source_team (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    source VARCHAR(32) NOT NULL,
    sport VARCHAR(32) NOT NULL,
    source_team_id VARCHAR(120) NOT NULL,
    source_team_name VARCHAR(200) NOT NULL,
    normalized_name VARCHAR(200) NOT NULL,
    country_code VARCHAR(20) NULL,
    raw_payload JSON NULL,
    first_seen_at DATETIME NULL,
    last_seen_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_source_team (source, sport, source_team_id),
    KEY idx_source_team_name (sport, normalized_name)
);

CREATE TABLE IF NOT EXISTS source_team_mapping (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    our_team_id BIGINT UNSIGNED NOT NULL,
    source VARCHAR(32) NOT NULL,
    sport VARCHAR(32) NOT NULL,
    source_team_id VARCHAR(120) NOT NULL,
    source_team_name VARCHAR(200) NOT NULL,
    normalized_name VARCHAR(200) NOT NULL,
    confidence DECIMAL(8,4) NOT NULL DEFAULT 0,
    status ENUM('seed_candidate', 'confirmed', 'needs_review', 'rejected', 'inactive') NOT NULL DEFAULT 'seed_candidate',
    evidence_count INT NOT NULL DEFAULT 0,
    source_event_count INT NOT NULL DEFAULT 0,
    confirmed_method VARCHAR(80) NULL,
    confirmed_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_source_team_mapping (source, sport, source_team_id),
    KEY idx_mapping_our_team (our_team_id),
    KEY idx_mapping_status (status, sport, confidence),
    CONSTRAINT fk_mapping_our_team FOREIGN KEY (our_team_id) REFERENCES our_team(id)
);

CREATE TABLE IF NOT EXISTS source_event (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    source VARCHAR(32) NOT NULL,
    sport VARCHAR(32) NOT NULL,
    source_event_id VARCHAR(120) NOT NULL,
    start_time DATETIME NULL,
    home_source_team_id VARCHAR(120) NOT NULL,
    home_source_team_name VARCHAR(200) NOT NULL,
    away_source_team_id VARCHAR(120) NOT NULL,
    away_source_team_name VARCHAR(200) NOT NULL,
    home_score INT NULL,
    away_score INT NULL,
    competition_id VARCHAR(120) NULL,
    competition_name VARCHAR(200) NULL,
    raw_payload JSON NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_source_event (source, sport, source_event_id),
    KEY idx_source_event_time (sport, start_time),
    KEY idx_source_event_home (source, sport, home_source_team_id),
    KEY idx_source_event_away (source, sport, away_source_team_id)
);

CREATE TABLE IF NOT EXISTS team_mapping_evidence (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    our_team_id BIGINT UNSIGNED NULL,
    source_a VARCHAR(32) NOT NULL,
    source_a_team_id VARCHAR(120) NOT NULL,
    source_b VARCHAR(32) NOT NULL,
    source_b_team_id VARCHAR(120) NOT NULL,
    source_a_event_id VARCHAR(120) NOT NULL,
    source_b_event_id VARCHAR(120) NOT NULL,
    sport VARCHAR(32) NOT NULL,
    evidence_type ENUM('event_match', 'anchor_propagation', 'llm_verified', 'manual_confirmed') NOT NULL DEFAULT 'event_match',
    event_match_score DECIMAL(8,4) NOT NULL DEFAULT 0,
    name_score DECIMAL(8,4) NULL,
    time_diff_minutes DECIMAL(10,2) NULL,
    score_match TINYINT NULL,
    side_match TINYINT NULL,
    home_away_reversed TINYINT NOT NULL DEFAULT 0,
    conflict_count INT NOT NULL DEFAULT 0,
    confidence DECIMAL(8,4) NOT NULL DEFAULT 0,
    status ENUM('candidate', 'confirmed', 'needs_review', 'rejected') NOT NULL DEFAULT 'candidate',
    details JSON NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_evidence_pair_event (
        source_a, source_a_team_id, source_b, source_b_team_id,
        source_a_event_id, source_b_event_id
    ),
    KEY idx_evidence_our_team (our_team_id),
    KEY idx_evidence_status (status, sport, confidence),
    CONSTRAINT fk_evidence_our_team FOREIGN KEY (our_team_id) REFERENCES our_team(id)
);

CREATE TABLE IF NOT EXISTS llm_verification (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    our_team_id BIGINT UNSIGNED NULL,
    proposal_key VARCHAR(120) NOT NULL,
    provider VARCHAR(32) NOT NULL,
    model VARCHAR(100) NOT NULL,
    same_team TINYINT NULL,
    confidence DECIMAL(8,4) NOT NULL DEFAULT 0,
    recommended_status ENUM('llm_verified', 'needs_review', 'reject') NOT NULL,
    risk_flags JSON NULL,
    reason TEXT NULL,
    request_payload JSON NULL,
    response_payload JSON NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_llm_verification (proposal_key, provider, model),
    KEY idx_llm_our_team (our_team_id),
    KEY idx_llm_status (recommended_status, confidence),
    CONSTRAINT fk_llm_our_team FOREIGN KEY (our_team_id) REFERENCES our_team(id)
);

CREATE TABLE IF NOT EXISTS team_alias (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    our_team_id BIGINT UNSIGNED NOT NULL,
    sport VARCHAR(32) NOT NULL,
    alias_name VARCHAR(200) NOT NULL,
    normalized_alias VARCHAR(200) NOT NULL,
    source VARCHAR(32) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_team_alias (sport, normalized_alias, our_team_id),
    KEY idx_team_alias_lookup (sport, normalized_alias),
    CONSTRAINT fk_alias_our_team FOREIGN KEY (our_team_id) REFERENCES our_team(id)
);

CREATE TABLE IF NOT EXISTS source_team_match_stats (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    run_id BIGINT UNSIGNED NOT NULL,
    source VARCHAR(32) NOT NULL,
    sport VARCHAR(32) NOT NULL,
    source_events_in_run INT NOT NULL DEFAULT 0,
    source_teams_in_run INT NOT NULL DEFAULT 0,
    total_source_teams INT NOT NULL DEFAULT 0,
    mapped_source_teams INT NOT NULL DEFAULT 0,
    unmapped_source_teams INT NOT NULL DEFAULT 0,
    mapped_ratio DECIMAL(10,6) NOT NULL DEFAULT 0,
    events_with_unmapped_team INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_source_team_match_stats_run_source (run_id, source, sport),
    KEY idx_source_team_match_stats_source (source, sport, mapped_ratio),
    CONSTRAINT fk_stats_pipeline_run FOREIGN KEY (run_id) REFERENCES pipeline_run(id)
);

CREATE OR REPLACE VIEW v_source_team_status AS
SELECT
    st.id AS source_team_row_id,
    st.source,
    st.sport,
    st.source_team_id,
    st.source_team_name,
    st.normalized_name,
    st.first_seen_at,
    st.last_seen_at,
    stm.our_team_id,
    stm.confidence AS mapping_confidence,
    stm.evidence_count,
    CASE
        WHEN stm.status = 'confirmed' THEN 'mapped'
        WHEN stm.status IN ('seed_candidate', 'needs_review') THEN 'pending_more_sources'
        WHEN stm.status IN ('rejected', 'inactive') THEN stm.status
        ELSE 'unmatched'
    END AS source_team_status,
    stm.status AS mapping_status,
    stm.confirmed_method,
    stm.confirmed_at
FROM source_team st
LEFT JOIN source_team_mapping stm
    ON stm.source = st.source
    AND stm.sport = st.sport
    AND stm.source_team_id = st.source_team_id;

CREATE OR REPLACE VIEW v_source_team_match_stats AS
SELECT
    id,
    run_id,
    source,
    sport,
    total_source_teams,
    mapped_source_teams AS mapped_teams,
    unmapped_source_teams AS unmatched_teams,
    unmapped_source_teams AS single_source_active_teams,
    events_with_unmapped_team,
    mapped_ratio,
    source_events_in_run,
    source_teams_in_run,
    created_at,
    updated_at
FROM source_team_match_stats;

CREATE OR REPLACE VIEW v_our_team_source_mapping AS
SELECT
    ot.id AS our_team_id,
    ot.sport,
    ot.canonical_name,
    ot.status AS our_team_status,
    ot.confidence AS our_team_confidence,
    GROUP_CONCAT(
        CASE WHEN stm.source = 'thesports' THEN stm.source_team_id END
        ORDER BY stm.source_team_id SEPARATOR ', '
    ) AS thesports_team_ids,
    GROUP_CONCAT(
        CASE WHEN stm.source = 'thesports' THEN stm.source_team_name END
        ORDER BY stm.source_team_name SEPARATOR ', '
    ) AS thesports_team_names,
    GROUP_CONCAT(
        CASE WHEN stm.source = 'sr' THEN stm.source_team_id END
        ORDER BY stm.source_team_id SEPARATOR ', '
    ) AS sr_team_ids,
    GROUP_CONCAT(
        CASE WHEN stm.source = 'sr' THEN stm.source_team_name END
        ORDER BY stm.source_team_name SEPARATOR ', '
    ) AS sr_team_names,
    GROUP_CONCAT(
        CASE WHEN stm.source = 'ls' THEN stm.source_team_id END
        ORDER BY stm.source_team_id SEPARATOR ', '
    ) AS ls_team_ids,
    GROUP_CONCAT(
        CASE WHEN stm.source = 'ls' THEN stm.source_team_name END
        ORDER BY stm.source_team_name SEPARATOR ', '
    ) AS ls_team_names,
    GROUP_CONCAT(
        CASE WHEN stm.source = 'bc' THEN stm.source_team_id END
        ORDER BY stm.source_team_id SEPARATOR ', '
    ) AS bc_team_ids,
    GROUP_CONCAT(
        CASE WHEN stm.source = 'bc' THEN stm.source_team_name END
        ORDER BY stm.source_team_name SEPARATOR ', '
    ) AS bc_team_names,
    COUNT(DISTINCT stm.source) AS mapped_source_count,
    GROUP_CONCAT(DISTINCT stm.source ORDER BY stm.source SEPARATOR ',') AS mapped_sources
FROM our_team ot
LEFT JOIN source_team_mapping stm ON stm.our_team_id = ot.id
GROUP BY ot.id, ot.sport, ot.canonical_name, ot.status, ot.confidence;
