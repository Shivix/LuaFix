local M = {}

local fix_tags = require("luafix.tags")
local tags = fix_tags.tags
local header_tags = fix_tags.header_tags
local repeating_group_tags = fix_tags.repeating_group_tags
local util = require("luafix.utility")
local vals = require("luafix.values")

local msg_mt = {}
msg_mt.__index = function(t, k)
    if type(k) == "number" then
        return rawget(t, k)
    end
    local tag = tags[k]
    if tag ~= nil then
        return rawget(t, tag)
    end
    error("Incorrect tag name: " .. k)
end
msg_mt.__newindex = function(t, k, v)
    if type(k) == "number" then
        return rawset(t, k, v)
    end
    local tag = tags[k]
    if tag ~= nil then
        return rawset(t, tag, v)
    end
    error("Incorrect tag name: " .. k)
end
msg_mt.__tostring = function(t)
    return util.soh_to_pipe(M.msg_to_fix(t))
end

local repeating_group = {}
local repeating_group_mt = { __index = repeating_group }

function repeating_group:append(...)
    local new_group = { ... }
    setmetatable(new_group, msg_mt)
    table.insert(self, new_group)
end

local function new_repeating_group(...)
    local result = { ... }
    setmetatable(result, repeating_group_mt)
    for _, group in ipairs { ... } do
        setmetatable(group, msg_mt)
    end
    return result
end

local function sweep_ladder(msg, qty, side)
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

local function fix_to_table(fix_msg)
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

local function msg_to_fix(msg)
    assert(getmetatable(msg) == msg_mt)
    local result = table_to_fix(msg)
    result = "8=" .. msg[8] .. "\1" .. "9=" .. #result .. "\1" .. result
    local checksum = util.calculate_checksum(result)
    return result .. "10=" .. checksum .. "\1"
end

M.InternalLogging = false

M.tags = fix_tags.tags
M.values = vals
M.msg_types = fix_tags.msg_types
M.msg_mt = msg_mt

M.new_repeating_group = new_repeating_group
M.msg_to_fix = msg_to_fix
M.fix_to_table = fix_to_table
M.sweep_ladder = sweep_ladder

M.calculate_checksum = util.calculate_checksum
M.id_generator = util.id_generator
M.pipe_to_soh = util.pipe_to_soh
M.soh_to_pipe = util.soh_to_pipe

return M
