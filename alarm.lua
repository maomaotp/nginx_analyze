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
end

function update_alarm()
	init_mysql()
	local sql = string.format( "insert into `s_alarm` (`inCallId`, `alarmId`, `alarmType`, `inCallType`, `incallTime`, `operator`, `groupId`, `alarmCondition`) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')", result.inCallId, result.alarmId, result.alarmType, result.inCallType, result.incallTime, result.operator, result.groupId, result.alarmCondition)

	ngx.say(sql)
	local res, err, errno, sqlstate = db:query(sql)
	if not res then
		ngx.say("error: ", err)
		return 
	end
	close_mysql()
end

function incrby_alarm()
	local day = string.sub(result.incallTime, 0, 8)
	local month = string.sub(result.incallTime, 0, 6)

	--预警类型
	local res,err = red:hincrby(string.format("alarmA:%s:day", result.alarmType), day, 1)
	res,err = red:hincrby(string.format("alarmA:%s:month", result.alarmType), month, 1)
	--业务类型
	res,err = red:hincrby(string.format("alarmB:%s:day", result.inCallType), day, 1)
	res,err = red:hincrby(string.format("alarmB:%s:month", result.inCallType), month, 1)
	--预警类型+业务类型
	res,err = red:hincrby(string.format("alarmC:%s:%s:day", result.alarmType, result.inCallType), day, 1)
	res,err = red:hincrby(string.format("alarmC:%s:%s:month", result.alarmType, result.inCallType), month, 1)

	--坐席质检报告
	--小组预警量
	res,err = red:hincrby(string.format("qualityA:%s:day", result.groupId), day, 1)
	res,err = red:hincrby(string.format("qualityA:%s:month", result.groupId), month, 1)
	--小组+预警类型
	res,err = red:hincrby(string.format("qualityB:%s:%s:day", result.groupId, result.alarmType), day, 1)
	res,err = red:hincrby(string.format("qualityB:%s:%s:month", result.groupId, result.alarmType), month, 1)
	--小组成员预警量
	res,err = red:hincrby(string.format("qualityC:%s:day", result.operator), day, 1)
	res,err = red:hincrby(string.format("qualityC:%s:month", result.operator), month, 1)
	--小组成员+预警类型
	res,err = red:hincrby(string.format("qualityD:%s:%s:day", result.operator, result.alarmType), day, 1)
	res,err = red:hincrby(string.format("qualityD:%s:%s:month", result.operator, result.alarmType), month, 1)

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

	local alarm = '{"inCallId":"call001","alarmId":"alarm001","alarmType":1, "inCallType":3, "incallTime":"201412011055", "operator":"seats001", "groupId":"group001", "alarmCondition":2}'
	result = cjson.decode(alarm)
	if not result then 
		ngx.say("json parse err") 
		return 
	end

	if not result.inCallId or not result.alarmId or not result.alarmType or not result.incallTime or not result.operator or not result.groupId or not result.alarmCondition then
		ngx.say("nil")
		return
	end

	if (result.alarmType == "") or (result.incallTime == "") or (result.inCallType == "") or (result.groupId == "") or (result.groupId == "") or (result.operator == "") then
		ngx.say("nil")
		return
	end

	update_alarm()
	incrby_alarm()
end

main()
