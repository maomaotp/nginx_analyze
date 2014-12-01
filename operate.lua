local cjson = require "cjson"
local redis = require "resty.redis"
local mysql = require "resty.mysql"

REDIS_SERVER_IP = "127.0.0.1"
REDIS_SERVER_PORT = 6379
REDIS_TIMEOUT = 1000


local MYSQL_HOST = "192.168.1.120"
local MYSQL_POST = 3306
local MYSQL_DATABASE = "ana"
local MYSQL_USER = "dba"
local MYSQL_PASSWD = "123456"

local DB_TIMEOUT = 2000  --2 sec
local MAX_SIZE = 1024*1024

function init_mysql()
	db = mysql:new()
	if not db then
	    return 10001
	end
	
	db:set_timeout(DB_TIMEOUT)
	local ok, err, errno, sqlstate = db:connect{
	    host = MYSQL_HOST,
	    port = MYSQL_POST,
	    database = MYSQL_DATABASE,
	    user = MYSQL_USER,
	    password = MYSQL_PASSWD,
	    max_packet_size = MAX_SIZE
	}
	
	if not ok then
	    ngx.say("failed to connect: ", err, ": ", errno, " ", sqlstate)
	    return 10001
	end
	ngx.say("connected to mysql.")
	return 0
end

function close_mysql()
	local ok, err = db:set_keepalive(30000, 100)
	if not ok then
		ngx.say("failed to set keepalive: ", err)
		return
	end
	return 0
end

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

function close_redis()
    -- put it into the connection pool of size 100,
    -- with 10 seconds max idle time
    local ok, err = red:set_keepalive(10000, 100)
    if not ok then
        ngx.say("failed to set keepalive: ", err)
        return
    end

    -- or just close the connection right away:
    -- local ok, err = red:close()
    -- if not ok then
    --     ngx.say("failed to close: ", err)
    --     return 10000
    -- end
end

function query_all()
	local res,err = red:mget("inland", "hotel", "other", "foreign")
	ngx.say(cjson.encode(res))

--	if not res then
--		ngx.say("redis res: ", err)
--	end
--	ngx.say(res)
end

function update_alarm()
	init_mysql()
	if not result.inCallId or not result.alarmId or not result.alarmType or not result.incallTime or not result.seatsId or not result.groupId or not result.alarmCondition then
		ngx.say("nil")
		return
	end
	local sql = string.format( "insert into `s_alarm` (`inCallId`, `alarmId`, `alarmType`, `inCallType`, `incallTime`, `seatsId`, `groupId`, `alarmCondition`) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')", result.inCallId, result.alarmId, result.alarmType, result.inCallType, result.incallTime, result.seatsId, result.groupId, result.alarmCondition)

	ngx.say(sql)
	local res, err, errno, sqlstate = db:query(sql)
	if not res then
		ngx.say("error: ", err)
		return 
	end
	close_mysql()
end

function incrby_operate()
	if (string.len(result.inCallType) == 0) or (string.len(result.incallTime) == 0) or (string.len(result.operateType) == 0) or (string.len(result.area) == 0) or (string.len(result.status) == 0) or (string.len(result.failType) == 0 or (string.len(result.sex)) == 0 or string.len(result.age) == 0) then
		ngx.say("operate is NULL")
		return
	end
	local day = string.sub(result.incallTime, 0, 8)
	local month = string.sub(result.incallTime, 0, 6)

	local hour = string.sub(result.incallTime, 9, 10)

	--所有业务统计
	res,err = red:hincrby("operate:day", day, 1)
	res,err = red:hincrby("operate:month", month, 1)

	--业务类型分布统计
	res,err = red:hincrby(string.format("%s:day", result.inCallType), day, 1)
	res,err = red:hincrby(string.format("%s:month", result.inCallType), month, 1)
	--操作类型统计
	res,err = red:hincrby(string.format("%s:day", result.operateType), day, 1)
	res,err = red:hincrby(string.format("%s:month", result.operateType), month, 1)
	--业务类型和操作类型统计
	res,err = red:hincrby(string.format("%s:%s:day", result.inCallType, result.operateType), day, 1)
	res,err = red:hincrby(string.format("%s:%s:month", result.inCallType, result.operateType), month, 1)
	--来电时段统计
	res,err = red:hincrby(string.format("%s:day", hour), day, 1)
	res,err = red:hincrby(string.format("%s:month", hour), month, 1)
	--来电地域统计
	res,err = red:hincrby(string.format("%s:%s:day", result.area, result.inCallType), day, 1)
	res,err = red:hincrby(string.format("%s:%s:month", result.area, result.inCallType), month, 1)
	--办理失败统计
	if (result.status == 0) then
		--失败原因分布统计
		res,err = red:hincrby(string.format("%s:day", result.failType), day, 1)
		res,err = red:hincrby(string.format("%s:month", result.failType), month, 1)
		--业务类型失败统计
		res,err = red:hincrby(string.format("%s:day", result.inCallType), day, 1)
		res,err = red:hincrby(string.format("%s:month", result.inCallType), month, 1)
	end
	--业务来电性别统计
	res,err = red:hincrby(string.format("%s:%s:day", result.sex, result.inCallType), day, 1)
	res,err = red:hincrby(string.format("%s:%s:month", result.sex, result.inCallType), month, 1)
	--业务来电年龄统计
	if (result.age <20) then
		res,err = red:hincrby(string.format("0~20:%s:day", result.inCallType), day, 1)
		res,err = red:hincrby(string.format("0~20:%s:month", result.inCallType), month, 1)
	elseif (result.age>=20 and result.age<30) then
		res,err = red:hincrby(string.format("20~30:%s:day", result.inCallType), day, 1)
		res,err = red:hincrby(string.format("20~30:%s:month", result.inCallType), month, 1)
	elseif (result.age>=30 and result.age<40) then
		res,err = red:hincrby(string.format("30~40:%s:day", result.inCallType), day, 1)
		res,err = red:hincrby(string.format("30~40:%s:month", result.inCallType), month, 1)
	elseif (result.age>=40 and result.age<50) then
		res,err = red:hincrby(string.format("40~50:%s:day", result.inCallType), day, 1)
		res,err = red:hincrby(string.format("40~50:%s:month", result.inCallType), month, 1)
	else (result.age>=50) then
		res,err = red:hincrby(string.format("50~:%s:day", result.inCallType), day, 1)
		res,err = red:hincrby(string.format("50~:%s:month", result.inCallType), month, 1)
	end

	close_redis()
end

function main()
	init_redis()
	ngx.req.read_body()
--	local args, err = ngx.req.get_post_args()
--	for key, val in pairs(args) do
--		ngx.say(key .. ":" .. args[key])
--	end
	local data = ngx.req.get_body_data()
	if not data then ngx.say("post data is nil") end

	local json_str = '{"inCallType":1,"incallTime":"201412011055", "operateType":2, "area":3, "status":1, "failType":2, "sex":1, "age":24}'
	result = cjson.decode(json_str)
	if not result then 
		ngx.say("json parse err") 
		return 
	end

	if not result.inCallType or not result.incallTime or not result.operateType or not result.area or not result.status or not result.failType or not result.sex or not result.age then
		ngx.say("operate nil")
		return
	end

	--update_alarm()
	incrby_operate()
end

main()
