local json 		= require('json')
local path 		= require('path')
local thread 	= require('thread')
local utils 	= require('utils')

local sqlite 	= require('sqlite3')

local DATA_TABLE 	= 'data'
local STAT_TABLE 	= 'stat'
local DATABASE_NAME = 'data.db'

local isInited = false
local data_stream_latest

--[[
传感器数据流和数据点记录和查询模块

这个模块使用 Sqlite 数据库来记录传感器数据流数据

--]]

local exports = {}

-- 统计周期
exports.HOUR  	= 'hour' -- 过云一小时内
exports.DAY  	= 'day'  -- 过去一天内
exports.WEEK  	= 'week' -- 过去一周期间
exports.MONTH  	= 'month'-- 过去一月期间
exports.YEAR  	= 'year' -- 过去一年期间

-- 统计方法
exports.AVG  	= 'avg'  -- 某时间段平均值
exports.MIN  	= 'min'  -- 某时间段的最小值
exports.MAX  	= 'max'  -- 某时间段的最大值

-- 数据流类型
local types = {}
types.temperature	= 101
types.humidity		= 120

exports.types   = types


local function paddingZero(value)
	if (value < 10) then
		return '0' .. value
	else
		return value
	end
end

-------------------------------------------------------------------------------
-- open the database

local function data_stream_open(name)
	if (not name) then
		name = DATABASE_NAME
	end

	local filename = name
	if (name ~= ':memory:') then
		local dirname  = exports.dirname or utils.dirname()
		filename = path.join(dirname, name)
	end

	local db = sqlite.open(filename)
	if (not db) then
		print('data_stream_open', 'open database failed', filename)
		return
	end

	-- print('database: ' .. filename)

	return db
end

-------------------------------------------------------------------------------
-- data stream type

local function data_stream_type(name)
	if (tonumber(name)) then
		return name

	else
		return types[name] or name
	end
end

-------------------------------------------------------------------------------
-- stat

local function data_stat_add(db, type, method, range, value, timestamp) 
	local sql = "INSERT INTO stat(type, method, range, value, timestamp)"
		.. " VALUES (?, ?, ?, ?, ?)"
	local stmt, err = db:prepare(sql)
	if (not stmt) then
		return stmt, err
	end

	stmt:bind(data_stream_type(type), method, range, value, timestamp)
	stmt:exec()
	stmt:close()
end

local function data_stat_get(db, type, method, range, startTime)
	local sql = "SELECT id,type,method,range,value,timestamp FROM stat"
		.. " WHERE type=? AND method=? AND range=? AND timestamp=?"
	local stmt, err = db:prepare(sql)
	if (not stmt) then
		return stmt, err
	end

	stmt:bind(type, method, range, startTime)
	local ret = stmt:first_row()

	stmt:close()

	return ret

end

local function data_stat_list(db, type, method, range, startTime, endTime)
	limit = limit or 20

	local where = nil
	local sql = "SELECT id,type,method,range,value,timestamp FROM stat"

	local stmt, err = db:prepare(sql)
	assert(stmt, err)

	local ret = {}

	local index = 0
	for row in stmt:rows() do
		table.insert(ret, row)

		index = index + 1
		if (index > limit) then
			break
		end
	end

	stmt:close()

	return ret
end

-------------------------------------------------------------------------------
-- add

local function data_stream_add(db, type, value, timestamp, interval) 
	if (tonumber(value) == nil) then
		return
	end

	-- check count
	if (timestamp) then
		local sql = "SELECT COUNT(id) AS id FROM data WHERE type=? AND timestamp=?"
		local stmt, err = db:prepare(sql)
		assert(stmt, err)

		stmt:bind(data_stream_type(type), timestamp)

		local count = stmt:first_cols() or 0
		stmt:close()

		if (count > 0) then
			return
		end
	end

	timestamp = timestamp or os.time()
	if (interval) then
		local ret = data_stream_latest(db, type)
		if (ret and ret.timestamp) then
			local span = math.abs(timestamp - ret.timestamp)
			if (span < interval) then
				--print('interval', span, interval)
				return
			end
		end
	end

	-- max id
	local sql = "SELECT MAX(id) AS id FROM data"
	local stmt, err = db:prepare(sql)
	assert(stmt, err)

	local id = (stmt:first_cols() or 0) + 1
	stmt:close()

	-- insert
	local sql = "INSERT INTO data(id, type, value, timestamp) VALUES (?, ?, ?, ?)"
	local stmt, err = db:prepare(sql)
	assert(stmt, err)

	stmt:bind(id, data_stream_type(type), value, timestamp)
	stmt:exec()
	stmt:close()
end

-------------------------------------------------------------------------------
-- clear

local function data_stream_clear(db)
	local sql = "SELECT * FROM data WHERE type=? AND value>?"
	local stmt = db:prepare(sql)
	if (not stmt) then
		return
	end

	stmt:bind(types.temperature, value or 50)

	for row in stmt:rows() do
		console.log(row)
	end

	stmt:close()

	db:exec("DELETE FROM data WHERE type=" .. types.temperature .. " AND value > 50")

	db:exec("DELETE FROM stat")

end

-------------------------------------------------------------------------------
-- exists

local function data_stream_exists(db, name)
	local sql = "SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name=?"
	local stmt = db:prepare(sql)
	local count = 0
	if (stmt) then
		stmt:bind(name)
		count = stmt:first_cols() or 0

		--print('data_stream_exists: count', count)
		stmt:close()
	end

	if (count > 0) then
		return true
	end
end

-------------------------------------------------------------------------------
-- find

local function data_stream_find_range(db, types, startTime, endTime)

	local now = os.time()
	endTime = endTime or now

	local isCurrent = false
	if (startTime <= now) and (now <= endTime) then
		--print('now is in', os.date(nil, startTime), os.date(nil, now))
		isCurrent = true
	end

	local result = {}

	for i = 1, #types do
		local key    = types[i]
		local tokens = key:split('.')

		local type   = data_stream_type(tokens[1])
		local method = tokens[2] or exports.AVG
		local range  = endTime - startTime

		local row = data_stat_get(db, type, method, range, startTime)
		if (row and not isCurrent) then
			result[key]  = row.value

		else
			local cols = 'COUNT(*) as count, ' .. method .. '(value) as value'

			local sql = "SELECT " .. cols .. " FROM data "
				.. "WHERE type=? AND timestamp>=? AND timestamp<? ORDER BY timestamp ASC"
			local stmt, err = db:prepare(sql)
			assert(stmt, err)

			stmt:bind(type, startTime, endTime)

			local row = stmt:first_row()
			if (row and row.value) then
				data_stat_add(db, type, method, range, row.value, startTime)
				result.count = row.count
				result[key]  = row.value
			end
		end
	end

	return result
end

local function data_stream_find_by_day(db, dataType, offset)
	local count = 24

	local date = os.date('*t')
	date.hour  = 0
	date.min   = 0
	date.sec   = 0

	offset = tonumber(offset)
	if (offset) then
		date.day = date.day + offset
	end

	local startTime = os.time(date)

	date.day = date.day + 1
	local endTime = os.time(date)

	local sql = "select avg(value) avg,max(value) max,min(value) min,count(value) count,"
		.. " min(timestamp) start"
		.. " from data"
		.. " where type=? and timestamp>=? and timestamp<?"
		.. " group by strftime('%Y-%m-%d-%H', timestamp, 'unixepoch', 'localtime')"
		.. " order by timestamp"

	local stmt, err = db:prepare(sql)
	assert(stmt, err)
	local dataId = data_stream_type(dataType)
	stmt:bind(dataId, startTime, endTime)

	local list = {}
	for row in stmt:rows() do
		local item = {}
		item[dataType] 			 = row.avg
		item[dataType .. '.min'] = row.min
		item[dataType .. '.max'] = row.max
		item['count'] 			 = row.count
		item['startTime'] 		 = row.start

		table.insert(list, item)
	end

	return list
end

local function data_stream_find_by_year(db, dataType, offset)
	local count = 12

	local date = os.date('*t')
	date.day  = 1
	date.hour = 0
	date.min  = 0
	date.sec  = 0
	date.month  = date.month - count -- + 1

	offset = tonumber(offset)
	if (offset) then
		date.year = date.year + offset
	end

	local startTime = os.time(date)

	date.year = date.year + 1
	local endTime = os.time(date)

	local sql = "select avg(value) avg,max(value) max,min(value) min,count(value) count,"
		.. " min(timestamp) start"
		.. " from data"
		.. " where type=? and timestamp>=? and timestamp<?"
		.. " group by strftime('%Y-%m', timestamp, 'unixepoch', 'localtime')"
		.. " order by timestamp"

	local stmt, err = db:prepare(sql)
	assert(stmt, err)
	local dataId = data_stream_type(dataType)
	stmt:bind(dataId, startTime, endTime)

	local list = {}
	for row in stmt:rows() do
		local item = {}
		item[dataType] 			 = row.avg
		item[dataType .. '.min'] = row.min
		item[dataType .. '.max'] = row.max
		item['count'] 			 = row.count
		item['startTime'] 		 = row.start

		table.insert(list, item)
	end

	return list
end

local function data_stream_find_by_month(db, dataType, offset)
	local count = 31

	local date = os.date('*t')
	date.day  = date.day - count -- + 1
	date.hour = 0
	date.min  = 0
	date.sec  = 0

	offset = tonumber(offset)
	if (offset) then
		date.month = date.month + offset
	end

	local startTime = os.time(date)

	date.month = date.month + 1
	local endTime = os.time(date)

	local sql = "select avg(value) avg,max(value) max,min(value) min,count(value) count,"
		.. " min(timestamp) start"
		.. " from data"
		.. " where type=? and timestamp>=? and timestamp<?"
		.. " group by strftime('%Y-%m-%d', timestamp, 'unixepoch', 'localtime')"
		.. " order by timestamp"

	local stmt, err = db:prepare(sql)
	assert(stmt, err)
	local dataId = data_stream_type(dataType)
	stmt:bind(dataId, startTime, endTime)

	local list = {}
	for row in stmt:rows() do
		local item = {}
		item[dataType] 			 = row.avg
		item[dataType .. '.min'] = row.min
		item[dataType .. '.max'] = row.max
		item['count'] 			 = row.count
		item['startTime'] 		 = row.start

		table.insert(list, item)
	end

	return list
end

local function data_stream_find_by_week(db, dataType, offset)
	local count = 7
	
	-- the time being of the date 7 days ago
	local date = os.date('*t')
	date.day  = date.day - count -- + 1
	date.hour = 0
	date.min  = 0
	date.sec  = 0

	offset = tonumber(offset)
	if (offset) then
		date.day = date.day + offset * 7
	end

	local startTime = os.time(date)

	date.day = date.day + 7
	local endTime = os.time(date)

	local sql = "select avg(value) avg,max(value) max,min(value) min,count(value) count,"
		.. " min(timestamp) start"
		.. " from data"
		.. " where type=? and timestamp>=? and timestamp<?"
		.. " group by strftime('%Y-%m-%d', timestamp, 'unixepoch', 'localtime')"
		.. " order by timestamp"

	local stmt, err = db:prepare(sql)
	assert(stmt, err)
	local dataId = data_stream_type(dataType)
	stmt:bind(dataId, startTime, endTime)

	local list = {}
	for row in stmt:rows() do
		local item = {}
		item[dataType] 			 = row.avg
		item[dataType .. '.min'] = row.min
		item[dataType .. '.max'] = row.max
		item['count'] 			 = row.count
		item['startTime'] 		 = row.start

		table.insert(list, item)
	end

	return list
end

local function data_stream_find(db, types, mode, offset)
	mode   = mode or 'day'
	offset = offset or 0

	--print('data_stream_find', 'mode', mode, 'offset', offset)

	if (mode == exports.DAY) then
		return data_stream_find_by_day(db, types, offset)

	elseif (mode == exports.YEAR) then
		return data_stream_find_by_year(db, types, offset)

	elseif (mode == exports.WEEK) then
		return data_stream_find_by_week(db, types, offset)

	elseif (mode == exports.MONTH) then
		return data_stream_find_by_month(db, types, offset)
	end
end

-------------------------------------------------------------------------------
-- import

local function data_stream_import(db, type)
	local db_old = data_stream_open('www/data.db')

	local sql = "SELECT * FROM sht20"

	local stmt, err = db_old:prepare(sql)
	assert(stmt, err)

	local index = 0
	local lastTime = process.uptime()

	for row in stmt:rows() do
		index = index + 1

		local temperature = row.temperature / 100
		local humidity    = row.humidity / 100
		--console.log(humidity)

		data_stream_add(db, 'temperature', temperature, row.time)
		data_stream_add(db, 'humidity',	   humidity,    row.time)

		--print(index)
		local now = process.uptime()
		if (now - lastTime) > 1 then
			print('index: ' .. index)
			lastTime = now
		end
	end

	stmt:close()
	db_old:close()

	print('total import: ' .. index)
end

-------------------------------------------------------------------------------
-- init

local function data_stream_init(db)
	if (isInited) then
		return
	end

	isInited = true

	if (not data_stream_exists(db, DATA_TABLE)) then
		local sql = "CREATE TABLE data (id INT, type INT, value TEXT, timestamp INT)"
		db:exec(sql)
	end

	if (not data_stream_exists(db, STAT_TABLE)) then
		local sql = "CREATE TABLE stat (id INT, type INT, method TEXT, range INT, value TEXT, timestamp INT, count INT)"
		db:exec(sql)
	end
end

function data_stream_latest(db, type) 
	local sql = "SELECT id, type, value, timestamp FROM data"
	if (type) then
		sql = sql .. ' WHERE (type=?)'
	end
	
	sql = sql .. ' ORDER BY timestamp DESC'


	local stmt, err = db:prepare(sql)
	assert(stmt, err)

	if (type) then
		stmt:bind(data_stream_type(type))
	end

	local ret = stmt:first_row()
	stmt:close()
	return ret
end

local function data_stream_list(db, type, limit)
	limit = limit or 20

	local sql = "SELECT id,type,value,timestamp FROM data"

	if (type) then
		sql = sql .. ' WHERE (type=?)'
	end
	
	local stmt, err = db:prepare(sql)
	assert(stmt, err)

	if (type) then
		--console.log(sql, type)
		stmt:bind(data_stream_type(type))
	end

	local ret = {}

	local index = 0
	for row in stmt:rows() do
		table.insert(ret, row)

		index = index + 1
		if (index > limit) then
			break
		end
	end

	stmt:close()

	return ret
end

local function data_stream_count(db, type, limit)
	local sql = "SELECT COUNT(*) FROM data"

	if (type) then
		sql = sql .. ' WHERE (type=?)'
	end
	
	local stmt, err = db:prepare(sql)
	assert(stmt, err)

	if (type) then
		--console.log(sql, type)
		stmt:bind(data_stream_type(type))
	end

	local ret = stmt:first_cols()

	stmt:close()

	return ret
end


-------------------------------------------------------------------------------
-- exports

-- name
-- mode
function exports.open(name)
	local db = data_stream_open(name)
	if (not db) then
		print('invalid database: ' .. tostring(name))
		return
	end

	db.import 	= data_stream_import
	db.latest 	= data_stream_latest
	db.list	 	= data_stream_list
	db.init 	= data_stream_init
	db.find 	= data_stream_find
	db.add 		= data_stream_add
	db.clear 	= data_stream_clear
	db.count 	= data_stream_count

	db.stat_list = data_stat_list
	db.stat_add  = data_stat_add

	return db
end

function exports.query(stream, mode, offset, options)
	local db = exports.open()
	if (not db) then 
		return
	end

    options = options or {}
    mode    = mode or 'month'
    offset  = offset or 0

    local data1  = {}
    local data2  = {}
    local data3  = {}
    local labels = {}
    local series = {}

    local chartData = { title = stream, labels = labels, series = series}

    local streams = {}

    if (options.max) then
        table.insert(series,  { name = "max",  data = data1 })
    end

    if (options.min) then
        table.insert(series,  { name = "min",  data = data2 })
    end

    if (#streams <= 0) then
    	--options.avg = true
    end

    if (options.avg) then
        table.insert(series,  { name = "avg",  data = data3 })
    end

    local ret = db:find(stream, mode, offset) or {}
    local count = #ret
    for i = 1, count do
        local item = ret[i]

        -- value
   		if (options.max) then
        	table.insert(data1, item[stream .. '.max'] or json.null)
        end

        if (options.min) then
        	table.insert(data2, item[stream .. '.min'] or json.null)
        end

        if (options.avg) then
            table.insert(data3, item[stream] or json.null)
        end

        -- label
        local date = os.date("*t", item.startTime)
        local label = ""
        if (mode == exports.DAY) then
            label = paddingZero(date.hour) .. ":00"

        elseif (mode == exports.YEAR) then
            label = date.year .. '-' .. paddingZero(date.month)

        else
            label = paddingZero(date.month) .. '-' .. paddingZero(date.day)
        end
        table.insert(labels, label)
    end

	db:close()
    return { ret = 0, data = chartData }
end

return exports
