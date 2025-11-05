# 修改以下变量以匹配你的环境；也可通过环境变量覆盖（SYMBOLS_CSV、BASE_TS、REDIS_HOST、REDIS_PORT）
REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-6379}
BASE_TS=${BASE_TS:-1640966400}                # 2022-01-01 00:00:00 Asia/Shanghai

# 支持用逗号分隔的环境变量传入 SYMBOLS，例如: SYMBOLS_CSV="ETH_USDT,BTC_USDT"
if [ -n "$SYMBOLS_CSV" ]; then
  IFS=',' read -r -a SYMBOLS <<< "$SYMBOLS_CSV"
else
  SYMBOLS=("ETH_USDT" "BTC_USDT")   # 与 conf 中 symbols 保持一致
fi

# Redis Lua：将 list 预扩容到指定长度
LUA='
local k=KEYS[1]
local target=tonumber(ARGV[1])
local val=ARGV[2]
local n=redis.call("LLEN", k)
if n < target then
  for i=n+1, target do
    redis.call("RPUSH", k, val)
  end
end
return redis.call("LLEN", k)
'

# 周期长度（2022）
declare -A LEN=(
  ["1min"]=525600
  ["5min"]=105120
  ["15min"]=35040
  ["30min"]=17520
  ["60min"]=8760
  ["4hour"]=2190
  ["1day"]=365
  ["1mon"]=12
)

# 预创建列表
for s in "${SYMBOLS[@]}"; do
  for t in 1min 5min 15min 30min 60min 4hour 1day; do
    key="market.${s}.kline.${t}.${BASE_TS}"
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" EVAL "$LUA" 1 "$key" "${LEN[$t]}" "0,0,0,0,0,0,0,0" | cat
  done
  # 月线（不分年分区，但代码会检查 2022 起始）
  key="market.${s}.kline.1mon.${BASE_TS}"
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" EVAL "$LUA" 1 "$key" "${LEN["1mon"]}" "0,0,0,0,0,0,0,0" | cat
done