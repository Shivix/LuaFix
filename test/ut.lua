local fix = require("luafix")
local mt = fix.MsgTypes
local tags = fix.Tags
local vals = fix.Values

local sess = fix.null_session()

local nos = sess:new_msg(mt.NewOrderSingle)

local id_gen = fix.id_generator()
print(id_gen())
print(id_gen())

nos.Price = 5.2
nos.ClOrdID = "IDIDID"
nos.Symbol = "EURUSD"
local expected_msg = "8=FIX.4.4|9=62|56=NULL_TARGET|35=D|49=NULL_SENDER|44=5.2|11=IDIDID|55=EURUSD|10=252|"
assert(fix.soh_to_pipe(fix.msg_to_fix(nos)) == expected_msg)
assert(tostring(nos) == expected_msg)

local expected_nos = fix.fix_to_table(fix.pipe_to_soh(expected_msg))
for key, value in pairs(nos) do
    assert(expected_nos[key] == tostring(value))
end
