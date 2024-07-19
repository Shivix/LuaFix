local socket = require("socket")

local fix = {}

local tags = require("luafix.tags")
fix.Tags = tags.tags
local header_tags = tags.header_tags
local repeating_group_tags = tags.repeating_group_tags
fix.Values = require("luafix.values")

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
msg_mt.__tostring = function(t)
    return fix.soh_to_pipe(fix.msg_to_fix(t))
end

local repeating_group = {}
local repeating_group_mt = { __index = repeating_group }

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
    OrderCancelRequest = "F",
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
local session_mt = {
    __index = session,
    __gc = function(self)
        if self.client then
            self.client:close()
        end
    end,
}

function fix.null_session()
    local new_sess = {}
    setmetatable(new_sess, session_mt)
    new_sess.sender_comp_id = "NULL_SENDER"
    new_sess.target_comp_id = "NULL_TARGET"
    return new_sess
end

function fix.test_session(fd)
    local new_sess = {}
    setmetatable(new_sess, session_mt)
    new_sess.sender_comp_id = "TEST_SENDER"
    new_sess.target_comp_id = "TEST_TARGET"

    for k, v in pairs(socket) do
        print(k)
    end
    new_sess.client = socket.tcp(fd)
    new_sess.client:settimeout(0)

    local logon = new_sess:new_msg(fix.MsgTypes.Logon)
    logon.HeartBtInt = 30
    logon.EncryptMethod = "N"
    new_sess:send(logon)
    new_sess:wait_for_msg(fix.MsgTypes.Logon)

    return new_sess
end

function fix.new_session(
    endpoint,
    port,
    sender_comp_id,
    target_comp_id,
    heartbeat_int,
    username,
    password
)
    -- TODO: basic version of new_msg outside of session that can use here and use in tests
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
fix.new_initiator = fix.new_session

function fix.new_acceptor(endpoint, port, sender_comp_id, target_comp_id, heartbeat_int)
    local new_sess = {}
    setmetatable(new_sess, session_mt)
    new_sess.sender_comp_id = sender_comp_id
    new_sess.target_comp_id = target_comp_id
    new_sess.server = socket.bind(endpoint, port)
    new_sess.client = new_sess.server:accept()
    new_sess.client:settimeout(0)
    new_sess:wait_for_msg(fix.MsgTypes.Logon)
    local logon = new_sess:new_msg(fix.MsgTypes.Logon)
    logon.HeartBtInt = heartbeat_int
    logon.EncryptMethod = "N"
    new_sess:send(logon)
    return new_sess
end

function session:send(msg)
    assert(getmetatable(msg) == msg_mt)
    assert(self.client ~= nil)
    self.client:send(fix.msg_to_fix(msg))
end
local function check_msg_type(data, msg_types)
    local rcvd_msg_type = get_msg_type(data)
    for _, msg_type in ipairs(msg_types) do
        if rcvd_msg_type == msg_type then
            return true
        end
    end
    return false
end

local function check_full_msg(data)
    return data:match("8=FIX") and data:match("10=[0-9]+\1")
end

function session:wait_for_msg(...)
    local msg_types = { ... }
    local data
    repeat
        data = ""
        repeat
            local chunk, err, partial = self.client:receive(100)
            if chunk then
                data = data .. chunk
            elseif partial then
                data = data .. partial
            else
                error(err)
            end
        until chunk == nil
    until check_msg_type(data, msg_types)
    if data ~= "" then
        log_msg(data)
    end
    return data
end

function session:get_next_msg()
    local data
    repeat
        data = ""
        repeat
            local chunk, err, partial = self.client:receive(100)
            if chunk then
                data = data .. chunk
            elseif partial then
                data = data .. partial
            else
                error(err)
            end
        until chunk == nil
    until check_full_msg(data)
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
function fix.pipe_to_soh(msg)
    local result, _ = string.gsub(msg, "|", "\1")
    return result
end

function fix.now()
    return os.date("%Y%m%d-%H:%M:%S.") .. math.floor(socket.gettime() * 1000) % 1000
end

function fix.sweep_ladder(msg, qty, side)
    -- should be multiple orders for each level
    assert(getmetatable(msg) == msg_mt)
    for _, group in ipairs(msg.NoMDEntries) do
        if tonumber(group.MDEntryType) == side then
            if group.MDEntryType <= qty then
                error("not yet implemented")
            else
                -- sweep (partial) for final time
                return
            end
            qty = qty - group.MDEntrySize
            print(group.MDEntryPx)
            print(group.MDEntrySize)
        end
    end
end

local function table_to_fix(msg)
    local result = ""
    -- handle header fields first
    for tag, value in pairs(msg) do
        if value ~= nil and header_tags[tag] and tag ~= 8 then
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
            if value ~= nil and not header_tags[tag] and type(value) ~= "function" then
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
        if repeating_group_tags[tag] ~= nil then
            current_repeat = repeating_group_tags[tag]
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
    assert(getmetatable(msg) == msg_mt)
    local result = table_to_fix(msg)
    result = "8=" .. msg[8] .. "\1" .. "9=" .. #result .. "\1" .. result
    local checksum = calculate_checksum(result)
    return result .. "10=" .. checksum .. "\1"
end

function fix.id_generator()
    local n = 0
    return function()
        local time = os.time()
        n = n + 1
        return time + n
    end
end

return fix
