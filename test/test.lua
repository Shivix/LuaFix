local fix = require("lib/luafix")

fix:create_session("localhost", 8080, "SENDER", "TARGET")

local nos = fix:new_msg(fix.MsgTypes.NewOrderSingle)
local nos2 = fix:new_msg(fix.MsgTypes.NewOrderSingle)

nos[Tags.Price] = 5.2
nos[Tags.ClOrdID] = "IDIDID"
nos[Tags.Symbol] = "EURUSD"
nos2[Tags.Price] = 1
nos2[Tags.Symbol] = "USDKRW"
nos2[Tags.Account] = "ACCOUNT"

local md = fix:new_msg(fix.MsgTypes.MarketDataSnapshot)

local md_entries = fix.new_repeating_group(
    { [Tags.MDEntryType] = 1, [Tags.MDEntryPx] = 1.321 },
    { [Tags.MDEntryType] = 1, [Tags.MDEntryPx] = 1.320 },
    { [Tags.MDEntryType] = 0, [Tags.MDEntryPx] = 1.330 }
)
md_entries:add_groups { [Tags.MDEntryType] = 0, [Tags.MDEntryPx] = 1.331 }
md.NoMDEntries = md_entries

print(nos.Price)
nos.Price = 5.3

print(fix.fix_to_pipe(fix.msg_to_fix(nos)))
print(fix.fix_to_pipe(fix.msg_to_fix(nos2)))
print(fix.fix_to_pipe(fix.msg_to_fix(md)))

fix:send(md)
fix:send(nos)
