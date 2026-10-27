生成压测临时数据文件：
# 重新生成请求体（ts 不加偏移，就是当前时间）
php -r '
$secret="test-secret-12345"; $c="test-1"; $count=200000; $lines=[];
for($i=0;$i<$count;$i++){
  $ts=time(); $n=bin2hex(random_bytes(12));
  $s=base64_encode(hash_hmac("sha256","{$c}|{$ts}|{$n}",$secret,true));
  $lines[]="clientid={$c}&username={$c}&password=".urlencode("{$s}|{$ts}|{$n}");
}
file_put_contents("/tmp/req_bodies.txt",implode("\n",$lines));
echo "Generated ".count($lines)." requests\n";
'

# 先 5 秒验证
wrk -t12 -c400 -d5s --script=bench_replay.lua --latency http://127.0.0.1:9503/mqtt/auth


# 手动验证
php -r '
$secret = "test-secret-12345";
$ts = time();
$nonce = bin2hex(random_bytes(8));
$payload = "test-1|{$ts}|{$nonce}";
$sig = base64_encode(hash_hmac("sha256", $payload, $secret, true));
echo "curl -X POST http://127.0.0.1:9503/mqtt/auth -H \"Content-Type: application/x-www-form-urlencoded\" -d \"clientid=test-1&username=test-1&password=" . urlencode($sig) . "%7C{$ts}%7C{$nonce}\"\n";
' | bash


压力测试：
wrk -t12 -c400 -d30s --script=bench_replay.lua --latency http://127.0.0.1:9503/mqtt/auth

wrk -t12 -c1000 -d5s --script=bench_replay.lua --latency http://127.0.0.1:9503/mqtt/auth