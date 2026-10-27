压测命令
bash
# 1. 生成请求体
php extensions/edge/test/bench_gen.php 200000 /tmp/bench_bodies.txt


# 2. 先跑一次小流量预热
wrk -t2 -c50 -d5s --script=extensions/edge/test/bench_wrk.lua http://127.0.0.1:9503/mqtt/auth

# 2. 然后正式压
wrk -t12 -c1000 -d30s --script=extensions/edge/test/bench_wrk.lua --latency http://127.0.0.1:9503/mqtt/auth