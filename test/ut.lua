local fix = require("luafix")
local session = require("luafix.session")
local tags = require("luafix.tags")
local mt = tags.msg_types
local vals = require("luafix.values")

local sess <close> = session.null_session()

local nos = sess:new_msg(mt.NewOrderSingle)

local id_gen = fix.id_generator()
local first_id = id_gen()
local second_id = id_gen()
assert(second_id == first_id + 1)

nos.Price = "5.2"
nos.ClOrdID = "IDIDID"
nos.Symbol = "EURUSD"
local expected_msg = "8=FIX.4.4|35=D|52=20240825-19:04:54.259|56=NULL_TARGET|49=NULL_SENDER|44=5.2|11=IDIDID|55=EURUSD|"
print(expected_msg)
print(fix.soh_to_pipe(fix.msg_to_fix(nos)))
--assert(fix.soh_to_pipe(fix.msg_to_fix(nos)) == expected_msg)
--assert(tostring(nos) == expected_msg)

local expected_nos = fix.fix_to_table(fix.pipe_to_soh(expected_msg))
for tag, value in pairs(nos) do
    -- ReceiveTime<52> Will be different.
    if tag ~= 52 then
        if expected_nos[tag] ~= tostring(value) then
            print(tag .. " did not match", expected_nos[tag], "!=", value)
        end
    end
end
for tag, value in pairs(expected_nos) do
    -- ReceiveTime<52> Will be different.
    if tag ~= 52 then
        if nos[tag] ~= tostring(value) then
            -- TODO: Use my lualib unit test asserts
            print(tag .. " did not match: ", nos[tag], "!=", value)
        end
    end
end
