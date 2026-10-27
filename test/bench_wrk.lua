-- bench_wrk.lua
-- wrk 读取预生成的 POST body 文件，逐行发送

local bodies = {}
local counter = 1

function init(args)
    -- 读取文件
    local file = io.open("/tmp/bench_bodies.txt", "r")
    if file == nil then
        print("ERROR: Cannot open /tmp/bench_bodies.txt")
        os.exit(1)
    end

    local count = 0
    for line in file:lines() do
        if line ~= "" then
            table.insert(bodies, line)
            count = count + 1
        end
        if count >= 200000 then break end
    end
    file:close()

    print("Loaded " .. #bodies .. " request bodies")
end

function request()
    counter = counter % #bodies + 1
    local body = bodies[counter]

    return wrk.format("POST", "/mqtt/auth", {
        ["Content-Type"] = "application/x-www-form-urlencoded",
        ["Content-Length"] = tostring(#body),
    }, body)
end

-- 每个线程初始化时调用
function setup(thread)
    -- 均匀分配起始位置
    thread:set("counter", math.random(1, 200000))
end
