drop table if exists t_seq;
CREATE TABLE t_seq (
	id BIGINT UNSIGNED auto_increment NOT NULL COMMENT 'primary key',
	vers BIGINT UNSIGNED DEFAULT 0 NOT NULL COMMENT 'version of change',
	biz_tag varchar(100) NOT NULL COMMENT 'biz tag',
	current_max_id BIGINT UNSIGNED DEFAULT 1 NOT NULL COMMENT 'version of change',
	last_updated DATETIME DEFAULT now() NOT NULL    COMMENT 'update time',
	CONSTRAINT t_seq_pk PRIMARY KEY (id),
	CONSTRAINT t_seq_unique UNIQUE KEY (biz_tag)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;