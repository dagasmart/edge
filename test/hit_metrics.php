<?php
declare(strict_types=1);

$fp = @fsockopen('127.0.0.1', 9503, $errno, $errstr, 3);
if (!$fp) {
    echo "FAIL: {$errstr}\n";
    echo "DEAD: {$errstr} (errno={$errno})\n";
    exit(1);
}
echo "ALIVE\n";

$req = "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1:9503\r\nConnection: close\r\n\r\n";
fwrite($fp, $req);

$resp = '';
while (!feof($fp)) {
    $resp .= fread($fp, 8192);
}
fclose($fp);

echo substr($resp, 0, 200) . "\n";
