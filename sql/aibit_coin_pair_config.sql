CREATE TABLE IF NOT EXISTS aibit_coin_pair_config (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  symbol VARCHAR(64) NOT NULL UNIQUE,
  price_scale INT NOT NULL,
  PRIMARY KEY (id),
  KEY idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 示例初始化
INSERT INTO aibit_coin_pair_config (symbol, price_scale)
VALUES ('ETH_USDT', 18)
ON DUPLICATE KEY UPDATE price_scale=VALUES(price_scale);

INSERT INTO aibit_coin_pair_config (symbol, price_scale)
VALUES ('BTC_USDT', 18)
ON DUPLICATE KEY UPDATE price_scale=VALUES(price_scale);