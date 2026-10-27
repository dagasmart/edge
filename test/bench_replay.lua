-- bench_replay.lua
local file_lines = {}

-- 读预生成的请求体
local function read_lines(path)
    local f = io.open(path, "r")
    if not f then return {} end
    local lines = {}
    for line in f:lines() do
        table.insert(lines, line)
    end
    f:close()
    return lines
end

wrk.init = function(args)
    file_lines = read_lines("/tmp/req_bodies.txt")
    if #file_lines == 0 then
        print("ERROR: no request bodies loaded")
    else
        print("Loaded " .. #file_lines .. " request bodies")
    end
end

local counter = 0

request = function()
    counter = counter + 1
    local body = file_lines[(counter % #file_lines) + 1]
    return wrk.format("POST", "/mqtt/auth", {
        ["Content-Type"] = "application/x-www-form-urlencoded",
        ["Content-Length"] = tostring(#body)
    }, body)
end

done = function(summary, latency, requests)
    io.write(string.format("\n=== Results ===\n"))
    io.write(string.format("Requests:  %d\n", summary.requests))
    io.write(string.format("Duration:  %.2fs\n", summary.duration / 1000000))
    io.write(string.format("RPS:       %.2f\n", summary.requests / (summary.duration / 1000000)))
    io.write(string.format("Errors:    %d (conn: %d, read: %d, write: %d, timeout: %d, status: %d)\n",
        summary.errors.connect + summary.errors.read + summary.errors.write + summary.errors.timeout + summary.errors.status,
        summary.errors.connect, summary.errors.read, summary.errors.write, summary.errors.timeout, summary.errors.status))
    io.write(string.format("Latency p50: %.2fms, p95: %.2fms, p99: %.2fms\n",
        latency:percentile(50) / 1000, latency:percentile(95) / 1000, latency:percentile(99) / 1000))
end