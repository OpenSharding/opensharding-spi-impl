-- MySQL init table SQL

# CREATE TABLE IF NOT EXISTS saga_snapshot(
#   id BIGINT AUTO_INCREMENT PRIMARY KEY,
#   transaction_id VARCHAR(255) null,
#   snapshot_id int null,
#   revert_context VARCHAR(255) null,
#   transaction_context VARCHAR(255) null,
#   create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#   INDEX transaction_snapshot_index(transaction_id, snapshot_id)
# )ENGINE=InnoDB DEFAULT CHARSET=utf8;
#
# CREATE TABLE IF NOT EXISTS saga_event(
#   id BIGINT AUTO_INCREMENT PRIMARY KEY,
#   saga_id VARCHAR(255) null,
#   type VARCHAR(255) null,
#   content_json TEXT null,
#   create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#   INDEX saga_id_index(saga_id)
# )ENGINE=InnoDB DEFAULT CHARSET=utf8

-- H2 init table SQL

CREATE TABLE IF NOT EXISTS saga_snapshot(
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  transaction_id VARCHAR(255) null,
  snapshot_id int null,
  revert_context VARCHAR(255) null,
  transaction_context VARCHAR(255) null,
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS transaction_snapshot_index ON saga_snapshot(transaction_id, snapshot_id);

CREATE TABLE IF NOT EXISTS saga_event(
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  saga_id VARCHAR(255) null,
  type VARCHAR(255) null,
  content_json TEXT null,
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS running_sagas_index ON saga_event (saga_id, type);

-- POSTGRE init table SQL

# CREATE TABLE IF NOT EXISTS saga_snapshot(
#   id BIGSERIAL PRIMARY KEY,
#   transaction_id VARCHAR(255) null,
#   snapshot_id int null,
#   revert_context VARCHAR(255) null,
#   transaction_context VARCHAR(255) null,
#   create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );
# CREATE INDEX IF NOT EXISTS transaction_snapshot_index ON saga_snapshot(transaction_id, snapshot_id);
#
# CREATE TABLE IF NOT EXISTS saga_event(
#   id BIGSERIAL PRIMARY KEY,
#   saga_id VARCHAR(255) null,
#   type VARCHAR(255) null,
#   content_json TEXT null,
#   create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# )
# CREATE INDEX IF NOT EXISTS running_sagas_index ON saga_event (saga_id, type);