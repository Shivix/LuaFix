local socket = require("socket")

local fix = require("luafix")
local tags = fix.tags
local mt = fix.msg_types

-- This module acts as an extension to luafix to avoid making socket a requirement for the rest of the library.
local M = {}

M.InternalLogging = false

local session = {}
local session_mt = {
    __index = session,
    __gc = function(self)
        if self.client then
            self.client:close()
        end
    end,
}

function M.null_session()
    local new_sess = {}
    setmetatable(new_sess, session_mt)
    new_sess.sender_comp_id = "NULL_SENDER"
    new_sess.target_comp_id = "NULL_TARGET"
    new_sess.seq_num = 0
    return new_sess
end

function M.test_session(fd)
    local new_sess = {}
    setmetatable(new_sess, session_mt)
    new_sess.sender_comp_id = "TEST_SENDER"
    new_sess.target_comp_id = "TEST_TARGET"

    for k, _ in pairs(socket) do
        print(k)
    end
    new_sess.client = socket.tcp(fd)
    new_sess.client:settimeout(0)

    local logon = new_sess:new_msg(mt.Logon)
    logon.HeartBtInt = 30
    logon.EncryptMethod = "N"
    new_sess:send(logon)
    new_sess:wait_for_msg(mt.Logon)

    return new_sess
end

--- Options: heartbeat_int, reset_seq_num, username, password
function M.new_session(
    endpoint,
    port,
    sender_comp_id,
    target_comp_id,
    options
)
    -- TODO: basic version of new_msg outside of session that can use here and use in tests
    local new_sess = {}
    setmetatable(new_sess, session_mt)
    new_sess.sender_comp_id = sender_comp_id
    new_sess.target_comp_id = target_comp_id
    new_sess.seq_num = 0
    -- initiator
    new_sess.client = socket.tcp()
    new_sess.client:settimeout(0)
    new_sess.client:connect(endpoint, port)
    local logon = new_sess:new_msg(mt.Logon)
    logon.HeartBtInt = options.heartbeat_int or "30"
    logon.EncryptMethod = "N"
    logon.ResetSeqNumFlag = options.reset_seq_num or "Y"
    logon.Username = options.username
    logon.Password = options.password
    new_sess:send(logon)
    new_sess:wait_for_msg(mt.Logon)
    return new_sess
end
M.new_initiator = M.new_session

function M.new_acceptor(endpoint, port, sender_comp_id, target_comp_id)
    local new_sess = {}
    setmetatable(new_sess, session_mt)
    new_sess.sender_comp_id = sender_comp_id
    new_sess.target_comp_id = target_comp_id
    new_sess.seq_num = 0
    new_sess.server = socket.bind(endpoint, port)
    new_sess.client = new_sess.server:accept()
    new_sess.client:settimeout(0)
    local inc_logon = fix.fix_to_table(new_sess:wait_for_msg(mt.Logon))
    local logon = new_sess:new_msg(mt.Logon)
    logon.HeartBtInt = inc_logon.HeartBtInt
    logon.EncryptMethod = "N"
    new_sess:send(logon)
    return new_sess
end

function session:send(msg)
    assert(getmetatable(msg) == fix.msg_mt)
    assert(self.client ~= nil)
    self.seq_num = self.seq_num + 1
    msg.MsgSeqNum = self.seq_num
    self.client:send(fix.msg_to_fix(msg))
end

local function get_msg_type(msg)
    return string.match(msg, "35=([^\1])")
end

local function log_msg(msg)
    if M.InternalLogging then
        print(fix.soh_to_pipe(msg))
    end
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
        [52] = M.now(),
    }
    setmetatable(msg, fix.msg_mt)
    return msg
end

function M.now()
    return os.date("%Y%m%d-%H:%M:%S.") .. math.floor(socket.gettime() * 1000) % 1000
end

return M
