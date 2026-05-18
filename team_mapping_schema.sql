-- Draft schema for team identity mapping.
-- Review before running anywhere. The scripts in this project do not execute
-- this file automatically.

CREATE TABLE IF NOT EXISTS our_team (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    sport VARCHAR(32) NOT NULL,
    canonical_name VARCHAR(200) NOT NULL,
    country_code VARCHAR(20) NULL,
    primary_competition_name VARCHAR(200) NULL,
    status ENUM('active', 'inactive', 'merged', 'unknown') NOT NULL DEFAULT 'active',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_our_team_sport_name (sport, canonical_name)
);

CREATE TABLE IF NOT EXISTS source_team (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    source VARCHAR(32) NOT NULL,
    sport VARCHAR(32) NOT NULL,
    source_team_id VARCHAR(100) NOT NULL,
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

CREATE TABLE IF NOT EXISTS team_match_candidate (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    candidate_key VARCHAR(255) NOT NULL,
    source_a VARCHAR(32) NOT NULL,
    source_a_event_id VARCHAR(100) NOT NULL,
    source_b VARCHAR(32) NOT NULL,
    source_b_event_id VARCHAR(100) NOT NULL,
    sport VARCHAR(32) NOT NULL,
    candidate_score DECIMAL(8, 4) NOT NULL,
    score_detail JSON NOT NULL,
    source_event JSON NOT NULL,
    candidate_event JSON NOT NULL,
    llm_model VARCHAR(100) NULL,
    llm_judgment JSON NULL,
    sofascore_url VARCHAR(500) NULL,
    review_status ENUM(
        'pending_llm',
        'needs_sofascore_verification',
        'manual_review',
        'sofa_verified',
        'confirmed',
        'rejected'
    ) NOT NULL DEFAULT 'pending_llm',
    reviewer VARCHAR(100) NULL,
    reviewed_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_candidate_key (candidate_key),
    KEY idx_candidate_status (review_status, sport, candidate_score),
    KEY idx_candidate_events (source_a, source_a_event_id, source_b, source_b_event_id)
);

CREATE TABLE IF NOT EXISTS team_mapping (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    our_team_id BIGINT UNSIGNED NOT NULL,
    source VARCHAR(32) NOT NULL,
    sport VARCHAR(32) NOT NULL,
    source_team_id VARCHAR(100) NOT NULL,
    source_team_name VARCHAR(200) NOT NULL,
    confidence DECIMAL(8, 4) NOT NULL,
    match_method VARCHAR(50) NOT NULL,
    evidence_candidate_id BIGINT UNSIGNED NULL,
    status ENUM('pending', 'confirmed', 'rejected', 'inactive') NOT NULL DEFAULT 'pending',
    reviewer VARCHAR(100) NULL,
    reviewed_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_team_mapping (source, sport, source_team_id),
    KEY idx_team_mapping_our_team (our_team_id, sport),
    CONSTRAINT fk_team_mapping_our_team FOREIGN KEY (our_team_id) REFERENCES our_team(id)
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
    CONSTRAINT fk_team_alias_our_team FOREIGN KEY (our_team_id) REFERENCES our_team(id)
);
