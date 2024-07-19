-- Standalone functions that do not require the larger part of luafix to work.
local M = {}

function M.calculate_checksum(msg)
    local checksum = 0
    for i = 1, #msg do
        checksum = checksum + string.byte(msg, i)
    end
    checksum = checksum % 256
    -- checksum always 3 digits
    local prefix = ""
    if checksum < 100 then
        prefix = "0"
    end
    if checksum < 10 then
        prefix = "00"
    end
    return prefix .. checksum
end

function M.soh_to_pipe(msg)
    local result, _ = string.gsub(msg, "\1", "|")
    return result
end
function M.pipe_to_soh(msg)
    local result, _ = string.gsub(msg, "|", "\1")
    return result
end

function M.id_generator()
    local n = 0
    return function()
        local time = os.time()
        n = n + 1
        return time + n
    end
end

return M
