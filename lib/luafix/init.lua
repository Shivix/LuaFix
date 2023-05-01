local socket = require("socket")

local fix = {}
fix.Tags = require("lib/luafix.tags")

fix.InternalLogging = false

local msg_mt = {}
msg_mt.__index = function(t, k)
    if type(k) == "number" then
        return rawget(t, k)
    end
    local tag = fix.Tags[k]
    if tag ~= nil then
        return rawget(t, tag)
    end
    error("Incorrect tag name: " .. k)
end
msg_mt.__newindex = function(t, k, v)
    if type(k) == "number" then
        return rawset(t, k, v)
    end
    local tag = fix.Tags[k]
    if tag ~= nil then
        return rawset(t, tag, v)
    end
    error("Incorrect tag name: " .. k)
end

local repeating_group = {}
local repeating_group_mt = { __index = repeating_group }

local header_fields = {
    [8] = true,
    [9] = true,
    [34] = true,
    [35] = true,
    [49] = true,
    [50] = true,
    [52] = true,
    [56] = true,
    [57] = true,
    [115] = true,
    [116] = true,
    [128] = true,
}

-- reference table for repeating groups
local repeating_group_fields = {
    [78] = {
        [79] = true,
        [80] = true,
        [467] = true,
        [661] = true,
    },
    [268] = {
        [15] = true,
        [58] = true,
        [126] = true,
        [269] = true,
        [270] = true,
        [271] = true,
        [272] = true,
        [273] = true,
        [276] = true,
        [290] = true,
        [299] = true,
        [432] = true,
    },
    [382] = {
        [375] = true,
        [337] = true,
        [437] = true,
        [438] = true,
        [655] = true,
    },
    [453] = {
        [447] = true,
        [448] = true,
        [452] = true,
     --[[ TODO: if type == table is sub group
        [802] = {
            [523] = true,
            [803] = true,
        },]]
    },
}

fix.MsgTypes = {
    Heartbeat = "0",
    TestRequest = "1",
    ResendRequest = "2",
    Reject = "3",
    SequenceReject = "4",
    Logout = "5",
    ExecutionReport = "8",
    OrderCancelReject = "9",
    QuoteStatusRequest = "a",
    Logon = "A",
    NewOrderSingle = "D",
    QuoteRequest = "R",
    Quote = "S",
    MarketDataRequest = "V",
    MarketDataSnapshot = "W",
    MarketDataIncrementalRefresh = "X",
    SecurityList = "y",
    MarketDataRequestReject = "Y",
    QuoteCancel = "Z",
}

local function get_msg_type(msg)
    return string.match(msg, "35=([^\1])")
end

local function log_msg(msg)
    if fix.InternalLogging then
        print(fix.soh_to_pipe(msg))
    end
end

local function calculate_checksum(msg)
    local checksum = 0
    for i = 1, #msg do
        checksum = checksum + string.byte(msg, i)
    end
    checksum = checksum % 256
    -- checksum always 3 digits
    local prefix = ""
    if checksum < 100 then
        prefix = "0"
    end
    if checksum < 10 then
        prefix = "00"
    end
    return prefix .. checksum
end

local session = {}
local session_mt = { __index = session }

function fix.new_session(
    endpoint,
    port,
    sender_comp_id,
    target_comp_id,
    heartbeat_int,
    username,
    password
)
    local new_sess = {}
    setmetatable(new_sess, session_mt)
    new_sess.sender_comp_id = sender_comp_id
    new_sess.target_comp_id = target_comp_id
    -- initiator
    new_sess.client = socket.tcp()
    new_sess.client:settimeout(0)
    new_sess.client:connect(endpoint, port)
    local logon = new_sess:new_msg(fix.MsgTypes.Logon)
    logon.HeartBtInt = heartbeat_int
    logon.EncryptMethod = "N"
    logon.Username = username
    logon.Password = password
    new_sess:send(logon)
    new_sess:wait_for_msg(fix.MsgTypes.Logon)
    return new_sess
end

function session:send(msg)
    self.client:send(fix.msg_to_fix(msg))
end

function session:wait_for_msg(msg_type, timeout_fn)
    local data = ""
    repeat
        data = ""
        repeat
            local chunk, err, partial = self.client:receive(100)
            if chunk then
                data = data .. chunk
            elseif partial then
                data = data .. partial
            elseif err == "timeout" then
                if timeout_fn ~= nil then
                    timeout_fn()
                end
            else
                error(err)
            end
        until chunk == nil
    until get_msg_type(data) == msg_type
    log_msg(data)
    return data
end

function session:new_msg(msg_type)
    local msg = {
        [8] = "FIX.4.4",
        -- Base on? Separate funcs for message types?
        [35] = msg_type,
        [49] = self.sender_comp_id,
        [56] = self.target_comp_id,
    }
    setmetatable(msg, msg_mt)
    return msg
end

function repeating_group:append(...)
    local new_group = { ... }
    setmetatable(new_group, msg_mt)
    table.insert(self, new_group)
end

function fix.new_repeating_group(...)
    local new_repeating_group = { ... }
    setmetatable(new_repeating_group, repeating_group_mt)
    for _, group in ipairs { ... } do
        setmetatable(group, msg_mt)
    end
    return new_repeating_group
end

function fix.soh_to_pipe(msg)
    local result, _ = string.gsub(msg, "\1", "|")
    return result
end

function fix.now()
    local now = os.time()
    return os.date("%Y%m%d-%H:%M:%S.") .. string.format("%03d", now % 1000)
end

local function table_to_fix(msg)
    local result = ""
    -- handle header fields first
    for tag, value in pairs(msg) do
        if value ~= nil and header_fields[tag] and tag ~= 8 then
            result = result .. tag .. "=" .. value .. "\1"
        end
    end
    for tag, value in pairs(msg) do
        -- repeating groups are tables
        if type(value) == "table" then
            if #value > 0 then
                result = result .. tag .. "=" .. #value .. "\1"
            end
            result = result .. table_to_fix(value)
        else
            if value ~= nil and not header_fields[tag] and type(value) ~= "function" then
                result = result .. tag .. "=" .. value .. "\1"
            end
        end
    end
    return result
end

function fix.fix_to_table(fix_msg)
    local result = {}
    setmetatable(result, msg_mt)

    local current_group = {}
    setmetatable(current_group, msg_mt)
    local repeat_group = { current_group }
    setmetatable(repeat_group, repeating_group_mt)
    local current_repeat = {}

    for field in fix_msg:gmatch("([^\1]+)\1") do
        local key, value = field:match("([^=]+)=(.+)")
        local tag = tonumber(key)
        if tag == nil then
            error("invalid tag found")
        end

        -- start of repeating group
        if repeating_group_fields[tag] ~= nil then
            current_repeat = repeating_group_fields[tag]
            result[tag] = repeat_group
        -- part of current repeating group.
        elseif current_repeat[tag] then
            -- end of current group
            if current_group[tag] ~= nil then
                repeat_group:append()
                current_group = repeat_group[#repeat_group]
            end
            current_group[tag] = value
        else
            result[tag] = value
        end
    end
    return result
end

function fix.msg_to_fix(msg)
    local result = table_to_fix(msg)
    result = "8=" .. msg[8] .. "\1" .. "9=" .. #result .. "\1" .. result
    local checksum = calculate_checksum(result)
    return result .. "10=" .. checksum .. "\1"
end

return fix
