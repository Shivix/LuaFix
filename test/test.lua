local fix = require("lib/luafix")
local mt = fix.MsgTypes
local tag = fix.Tags

fix.InternalLogging = true

local md_sess = fix.new_session("localhost", 8080, "SENDERMD", "TARGETMD", 30, "user", "password")
local order_sess = fix.new_session("localhost", 8081, "SENDEROR", "TARGETOR", 30, "user", "password")

local mdr = md_sess:new_msg(mt.MarketDataRequest)
local md_entries = fix.new_repeating_group(
    { [tag.MDEntryType] = 1, [tag.MDEntryPx] = 1.321 },
    { [tag.MDEntryType] = 1, [tag.MDEntryPx] = 1.320 },
    { [tag.MDEntryType] = 0, [tag.MDEntryPx] = 1.330 }
)
md_entries:add_groups { [fix.Tags.MDEntryType] = 0, [tag.MDEntryPx] = 1.331 }
assert(#md_entries == 4)
md_sess:send(mdr)

local md = md_sess:wait_for_msg(fix.MsgTypes.MarketDataSnapshot)
md = fix.fix_to_table(md)

local nos = order_sess:new_msg(fix.MsgTypes.NewOrderSingle)

nos.Price = 5.2
nos.ClOrdID = "IDIDID"
nos.Symbol = "EURUSD"
assert(
    fix.soh_to_pipe(fix.msg_to_fix(nos))
            == "8=FIX.4.4|9=56|56=TARGETOR|35=D|49=SENDEROR|44=5.2|11=IDIDID|55=EURUSD|10=013|"
)
order_sess:send(nos);

--for k, v in pairs(md) do
    --print(k, v)
--end
