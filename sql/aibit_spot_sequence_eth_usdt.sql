CREATE TABLE IF NOT EXISTS aibit_spot_sequence_btc_usdt (
  id BIGINT UNSIGNED NOT NULL,                -- 序列ID（递增）
  type INT NOT NULL,                          -- 订单类型编码（见 puller 常量）
  order_id BIGINT NOT NULL,
  amount DECIMAL(36,18) NOT NULL,
  price DECIMAL(36,18) NOT NULL,
  circuit_rate DECIMAL(36,18) NOT NULL DEFAULT 0,
  created_at BIGINT NOT NULL,                 -- 建议毫秒时间戳
  user_id BIGINT NOT NULL,
  stp TINYINT NOT NULL DEFAULT 0,             -- 自成交保护策略
  taker VARCHAR(32) NOT NULL DEFAULT '',
  maker VARCHAR(32) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  KEY idx_user (user_id),
  KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;