local values = {}

values.Side = { Buy = 1, Sell = 2 }
values.OrdType = { Market = 1, Limit = 2, PrevQuoted = 'D' }
values.TimeInForce = { Day = 0, GTC = 1, IOC = '3', FOK = '4' }

return values
