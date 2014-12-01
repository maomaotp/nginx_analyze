local cjson = require "cjson"
local redis = require "resty.redis"

REDIS_SERVER_IP = "127.0.0.1"
REDIS_SERVER_PORT = 6379
REDIS_TIMEOUT = 1000

function init_redis()
	red = redis:new()
    red:set_timeout(REDIS_TIMEOUT) -- 1 sec

    local ok, err = red:connect(REDIS_SERVER_IP, REDIS_SERVER_PORT)
    if not ok then
        ngx.say("failed to connect: ", err)
        return 20001
    end

	return 0
end

function error_res(err_code)	
	local describe = "describe"
	local res_json = {errorId=err_code, desc = describe}
	ngx.say(cjson.encode(res_json))
end

function close_redis()

    -- put it into the connection pool of size 100,
    -- with 10 seconds max idle time
--    local ok, err = red:set_keepalive(10000, 100)
--    if not ok then
--        ngx.say("failed to set keepalive: ", err)
--        return
--    end

    -- or just close the connection right away:
     local ok, err = red:close()
     if not ok then
         ngx.say("failed to close: ", err)
         return 10000
     end
end


function parse_postargs()
	ngx.req.read_body()
	ngx.say("test");
	args = ngx.req.get_post_args()
	if not args then
		--ngx.say("failed to get post args: ", err)
		return 10002 
	end
	
	--解析翻页参数
	start = tonumber(args["start"])
	page = tonumber(args["page"])
	ngx.say(">>>" .. args["start"] .. ">>>" .. args["type"].. ">>>" .. args["opname"])

	if not start then start = 0 end
	if not page then page = -1 end
	
	opname = args["opName"]
	if not opname then
		--ngx.say("optype error")
		return 10002
	end
	return 0
end

function query_top(key)
	if not key then
		return 10000
	end
    local res, err = red:zrange(key, start, start+page)
    if not res then
        ngx.say("failed to get dog: ", err)
        return 10000
    end

	ngx.say( cjson.encode(res) )
	return 0
end

function update_top(key, id, score)
	if ( (key== nil) or (id == nil) or (score == nil) ) then
		return 12000
	end

	local res, err = red:zincrby(key, score, id)
	if err then
		ngx.say("failed to zincrby")
		return 1000
	end

	--ngx.say( cjson.encode(res) )
	return 0
end

function main()
	res_code = init_redis()
	if( res_code ~= 0 ) then
		error_res(res_code)
	end

	res_code = parse_postargs()
	if( res_code ~= 0 ) then
		error_res(res_code)
	end

	if (opname == "queryTop") then
		local key = args["key"]
		res_code = query_top(key)
		if( res_code ~= 0) then
			error_res(res_code)
			return
		end
	elseif (opname == "updateTop") then
		local key = args["key"]
		local id = args["id"]
		local score = args["score"]
		res_code = update_top(key, id, score)
		if (res_code ~= 0) then
			error_res(res_code)
			return
		end
	end

	close_redis()
end

main()
