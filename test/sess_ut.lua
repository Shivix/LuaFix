local fix = require("luafix")
local posix = require("posix")
local mt = fix.MsgTypes
local tags = fix.Tags
local vals = fix.Values

local sys_sock = require "posix.sys.socket"
local read, write = sys_sock.socketpair(sys_sock.AF_INET, sys_sock.SOCK_STREAM, 0)
-- are these actually read/write only?
print("TEST: ", read, write)

local read_sess = fix.test_session(read)
local write_sess = fix.test_session(write)

local mdr = write_sess:new_msg(mt.MarketDataRequest)
mdr.MarketDepth = 0
mdr.SubscriptionRequestType = 0
mdr.NoMDEntryTypes = fix.new_repeating_group(
    { [tags.MDEntryType] = 1 },
    { [tags.MDEntryType] = 0 }
)
write_sess:send(mdr)

print("next: " .. read_sess:get_next_msg())

local nos = write_sess:new_msg(mt.NewOrderSingle)
nos.Price = 5.2
nos.ClOrdID = "IDIDID"
nos.Symbol = "EURUSD"

local expected = "8=FIX.4.4|9=56|56=TARGETOR|35=D|49=SENDEROR|44=5.2|11=IDIDID|55=EURUSD|10=013|"
assert(
    fix.soh_to_pipe(fix.msg_to_fix(nos))
        == expected
)
write_sess:send(nos)

assert(read_sess:get_next_msg() == expected)
