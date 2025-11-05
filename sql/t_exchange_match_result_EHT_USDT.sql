CREATE TABLE IF NOT EXISTS t_exchange_match_result_btc_usdt (
  f_id BIGINT UNSIGNED NOT NULL,                    -- MR序列ID（唯一）
  symbol VARCHAR(64) NOT NULL,
  ts BIGINT NOT NULL,                               -- 建议毫秒时间戳
  order_type VARCHAR(32) NOT NULL,                  -- buy-limit/sell-market/submit-cancel/...
  role VARCHAR(32) NOT NULL,                        -- maker/taker/cancel/batch-cancel
  mr LONGTEXT NOT NULL,                             -- 原始撮合结果JSON
  extra LONGTEXT NOT NULL,                          -- 批量撤单等扩展JSON
  PRIMARY KEY (f_id),
  KEY idx_symbol_ts (symbol, ts),
  KEY idx_order_type (order_type),
  KEY idx_role (role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;