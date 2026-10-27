压测命令
bash

kill掉端口进程pid:

fuser -k 9503/tcp

# 1\. 生成请求体

php extensions/edge/test/bench\_gen.php 200000 /tmp/bench\_bodies.txt

# 1.1 数据指标

watch -n 1 'curl -s http://127.0.0.1:9503/health'

# 2\. 先跑一次小流量预热

wrk -t2 -c50 -d5s --script=extensions/edge/test/bench\_wrk.lua http://127.0.0.1:9503/mqtt/auth

# 2\. 然后正式压

wrk -t12 -c1000 -d30s --script=extensions/edge/test/bench\_wrk.lua --latency http://127.0.0.1:9503/mqtt/auth











wrk -t8 -c10000 -d30s --latency -s <(echo " wrk.method = \\"POST\\"

wrk.headers\[\\"Content-Type\\"] = \\"application/x-www-form-urlencoded\\"

wrk.body = \\"clientid=bench\_client\&username=bench\_user\&password=stY7vYGWNVdYetpUwe-db1UAS4fzzSc7kkNSVU\_CqFM\~|1793115998|532e2f6895e13603\&ipaddr=127.0.0.1\\"

") http://127.0.0.1:9503/mqtt/auth





落地后的操作清单

bash

\# 1. 覆盖文件

cp server.php helper.php hmac.php auth.php extensions/edge/bin/



\# 2. 确认文件无 CRLF

find extensions/edge/bin -name "\*.php" -exec sed -i 's/\\r$//' {} \\;

file extensions/edge/bin/\*.php   # 不应出现 CRLF



\# 3. 确认 Redis key 存在

redis-cli -p 16379 -a uApDVQD7 -n 2 SET "device:secret:bench\_client:bench\_user" "bench\_secret"



\# 4. 启动 4 实例

bash extensions/edge/bin/start-cluster.sh 4



\# 5. 用实时签名压测（全部应返回 200）

BODY=$(php -r '

$clientId="bench\_client";$username="bench\_user";$secret="bench\_secret";

$timestamp=time();$nonce=bin2hex(random\_bytes(8));

$payload=$clientId."|".$username."|".$timestamp."|".$nonce;

$sign=base64\_encode(hash\_hmac("sha256",$payload,$secret,true));

$sign\_encoded=strtr($sign,"+/=","-\_\~");

echo "clientid=".$clientId."\&username=".$username."\&password=".$sign\_encoded."|".$timestamp."|".$nonce."\&ipaddr=127.0.0.1";

')



wrk -t8 -c500 -d30s --latency -s <(echo "

wrk.method = \\"POST\\"

wrk.headers\[\\"Content-Type\\"] = \\"application/x-www-form-urlencoded\\"

wrk.body = \\"$BODY\\"

") http://127.0.0.1:9503/mqtt/auth

