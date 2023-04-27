local fix = require("lib/luafix")

local order_sess = fix.create_session("localhost", 8080, "SENDER", "TARGET", 30, "user", "password")

local nos = order_sess:new_msg(fix.MsgTypes.NewOrderSingle)

nos.Price = 5.2
nos.ClOrdID = "IDIDID"
nos.Symbol = "EURUSD"

local md = order_sess:wait_for_msg(fix.MsgTypes.MarketDataSnapshot)
md = fix.fix_to_table(md)

print(md.BeginString)
print(md.MDEntryPx)

order_sess:send(nos)
