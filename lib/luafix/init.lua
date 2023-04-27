Tags = require("lib/luafix.tags")

local socket = require("socket")

local fix = {}

local msg_mt = {}
msg_mt.__index = function(t, k)
    if type(k) == "number" then
        return rawget(t, k)
    end
    local tag = Tags[k]
    if tag ~= nil then
        return rawget(t, tag)
    end
    error("Incorrect tag name: " .. k)
end
msg_mt.__newindex = function(t, k, v)
    if type(k) == "number" then
        return rawset(t, k, v)
    end
    local tag = Tags[k]
    if tag ~= nil then
        return rawset(t, tag, v)
    end
    error("Incorrect tag name: " .. k)
end
local group_mt = {}

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

local session = {}

function fix.create_session(
    endpoint,
    port,
    sender_comp_id,
    target_comp_id,
    heartbeat_int,
    username,
    password
)
    local new_sess = session
    new_sess.sender_comp_id = sender_comp_id
    new_sess.target_comp_id = target_comp_id
    -- initiator
    new_sess.client = socket.tcp()
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

local function get_msg_type(msg)
    return string.match(msg, "35=([^\1])")
end

-- TODO: Ensure full messages are received? Is this necessary with TCP?
function session:wait_for_msg(msg_type)
    local msg, err
    repeat
        -- blocking
        msg, err = self.client:receive()
        assert(not err, err)
        print("incoming:", fix.soh_to_pipe(msg))
    until get_msg_type(msg) == msg_type
    return msg
end

function session:new_msg(msg_type)
    local msg = {
        [8] = "FIX.4.4",
        -- Base on? Separate funcs for message types?
        [35] = msg_type,
        -- based on some session config or something?
        [49] = self.sender_comp_id,
        [56] = self.target_comp_id,
    }
    setmetatable(msg, msg_mt)
    -- could check msg_type and add fields in needed like Account<1> from self.account
    return msg
end

function fix.new_repeating_group(...)
    local repeating_group = { ... }
    function repeating_group:add_groups(...)
        for _, group in ipairs { ... } do
            table.insert(self, group)
        end
    end
    setmetatable(repeating_group, group_mt)
    return repeating_group
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

function fix.msg_to_fix(msg)
    local result = table_to_fix(msg)
    result = "8=" .. msg[8] .. "\1" .. "9=" .. #result .. "\1" .. result
    local checksum = calculate_checksum(result)
    return result .. "10=" .. checksum .. "\1"
end

return fix
