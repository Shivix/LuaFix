local fix = require("luafix")
local session = require("luafix.session")
local mt = fix.msg_types
local tags = fix.tags
local vals = fix.values

fix.InternalLogging = true

local md_sess <close> = session.new_session("localhost", 8080, "SENDERMD", "TARGETMD", { username = "user", password = "password" })
local order_sess <close> =
    session.new_session("localhost", 8081, "SENDEROR", "TARGETOR", { username = "user", password = "password"})

local mdr = md_sess:new_msg(mt.MarketDataRequest)
mdr.MarketDepth = 0
mdr.SubscriptionRequestType = 0
mdr.NoMDEntryTypes = fix.new_repeating_group(
    { [tags.MDEntryType] = 1 },
    { [tags.MDEntryType] = 0 }
)

md_sess:send(mdr)

print("next: " .. md_sess:get_next_msg())

--local md = md_sess:wait_for_msg(mt.MarketDataSnapshot)
--print(md)
--md = fix.fix_to_table(md)

local test_md = md_sess:new_msg()
test_md.NoMDEntries = fix.new_repeating_group(
    { [tags.MDEntryType] = 0, [tags.MDEntryPx] = 1.1, [tags.MDEntrySize] = 500000 },
    { [tags.MDEntryType] = 0, [tags.MDEntryPx] = 1.11, [tags.MDEntrySize] = 1000000 },
    { [tags.MDEntryType] = 0, [tags.MDEntryPx] = 1.12, [tags.MDEntrySize] = 2000000 },
    { [tags.MDEntryType] = 0, [tags.MDEntryPx] = 1.13, [tags.MDEntrySize] = 3000000 },
    { [tags.MDEntryType] = 0, [tags.MDEntryPx] = 1.15, [tags.MDEntrySize] = 5000000 },
    { [tags.MDEntryType] = 1, [tags.MDEntryPx] = 1.09, [tags.MDEntrySize] = 500000 },
    { [tags.MDEntryType] = 1, [tags.MDEntryPx] = 1.08, [tags.MDEntrySize] = 1000000 },
    { [tags.MDEntryType] = 1, [tags.MDEntryPx] = 1.07, [tags.MDEntrySize] = 2000000 },
    { [tags.MDEntryType] = 1, [tags.MDEntryPx] = 1.06, [tags.MDEntrySize] = 3000000 },
    { [tags.MDEntryType] = 1, [tags.MDEntryPx] = 1.05, [tags.MDEntrySize] = 5000000 }
)
--fix.sweep_ladder(test_md, 3000000, vals.Side.Buy)

local nos = order_sess:new_msg(mt.NewOrderSingle)

nos.Price = 5.2
nos.ClOrdID = "IDIDID"
nos.Symbol = "EURUSD"
assert(
    fix.soh_to_pipe(fix.msg_to_fix(nos))
        == "8=FIX.4.4|9=56|56=TARGETOR|35=D|49=SENDEROR|44=5.2|11=IDIDID|55=EURUSD|10=013|"
)
order_sess:send(nos)
assert(order_sess.seq_num == 2)
order_sess:send(nos)
assert(order_sess.seq_num == 3)

print(session.now())
