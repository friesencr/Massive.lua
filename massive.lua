--
-- LUA PROMISE
--

local function null_or_unpack(val)
	if val then
		return unpack(val)
	else
		return nil
	end
end

local Promise = {

	--
	-- server functions
	--

	reject = function(self, ...)
		local arg = {...}
		assert(self:state() == 'pending')
		self._value = arg
		self._state = 'rejected'

		for i,v in ipairs(self._callbacks) do
			if v.event == 'always' or v.event == 'fail' then
				v.callback(null_or_unpack(arg))
			end
		end
		self._callbacks = {}
	end

	, resolve = function(self, ...)
		local arg = {...}
		assert(self:state() == 'pending')
		self._value = arg
		self._state = 'resolved'

		for i,v in ipairs(self._callbacks) do
			if v.event == 'always' or v.event == 'done' then
				v.callback(null_or_unpack(arg))
			end
		end
		self._callbacks = {}
	end

	, notify = function(self, ...)
		local arg = {...}
		assert(self:state() == 'pending')
		for i,v in ipairs(self._callbacks) do
			if v.event == 'progress' then
				v.callback(null_or_unpack(arg))
			end
		end
	end


	--
	-- client function
	--

	, always = function(self, callback)
		if self:state() ~= 'pending' then
			callback(null_or_unpack(self._value))
		else
			table.insert(self._callbacks, { event = 'always', callback = callback })
		end
		return self
	end

	, done = function(self, callback)
		if self:state() == 'resolved' then
			callback(null_or_unpack(self._value))
		elseif self:state() == 'pending' then
			table.insert(self._callbacks, { event = 'done', callback = callback })
		end
		return self
	end

	, fail = function(self, callback)
		if self:state() == 'rejected' then
			callback(null_or_unpack(self._value))
		elseif self:state() == 'pending' then
			table.insert(self._callbacks, { event = 'fail', callback = callback })
		end
		return self
	end

	, progress = function(self, callback)
		if self:state() == 'pending' then
			table.insert(self._callbacks, { event = 'progress', callback = callback })
		end
		return self
	end


	--
	-- utility functions
	--

	, state = function(self)
		return self._state
	end

}

function Promise:new()
	local obj = {
		is_deferred = true,
		_state = 'pending',
		_callbacks = {}
	}
	for k,v in pairs(Promise) do obj[k] = v end
	obj.new = nil
	return obj
end

setmetatable(Promise, { __call = function(x, ...) return Promise:new(...) end })

local function when(...)
	local arg = {...}
	local deferred = Promise:new()
	local returns = {}
	local total = # arg
	local completed = 0
	local failed = 0
	for i,v in ipairs(arg) do
		if (v and type(v) == 'table' and v.is_deferred) then
			local promise = v
			v:always(function(val)
				if promise:state() == 'rejected' then
					failed = failed + 1
				end
				completed = completed + 1
				returns[i] = val
			end)
		else
			returns[i] = v
			completed = completed + 1
		end
		if completed == total then
			if failed > 0 then
				deferred:reject(null_or_unpack(returns))
			else
				deferred:resolve(null_or_unpack(returns))
			end
		end
	end
	return deferred
end

--
-- END LUA PROMISE
--

-- package.loadlib("lsqlite3.dll", "luaopen_lsqlite3")()
require 'lsqlite3'

Massive = {}

local Table = {}
local Query = {}
local SQLite = {}
local Util = {}

Table.__index = Table
Query.__index = Query
SQLite.__index = SQLite

local function get_keys(t)
	local keys = {}
	for k,v in pairs(t) do
		table.insert(keys, k)
	end
	return keys
end
 
local function get_values(t)
	local values = {}
	for k,v in pairs(t) do
		table.insert(values, v)
	end
	return values
end

local string_split = function(s, p)
	assert(s)
	local temp = {}
	local index = 0
	local last_index = string.len(s)

	while true do
		local i, e = string.find(s, p, index)

		if i and e then
			local next_index = e + 1
			local word_bound = i - 1
			table.insert(temp, string.sub(s, index, word_bound))
			index = next_index
		else
			if index > 0 and index <= last_index then
				table.insert(temp, string.sub(s, index, last_index))
			elseif index == 0 then
				temp = {s}
			end
			break
		end
	end

	return temp
end

local function string_trim(s)
  return (s:gsub("^%s*(.-)%s*$", "%1"))
end

Massive.Util = {}
Massive.Util.get_keys = get_keys
Massive.Util.get_values = get_values
Massive.Util.string_split = string_split
Massive.Util.string_trim = string_trim

function Table:new(tableName, pk, _db)
	local obj = {}
	obj.name = tableName
	obj.pk = pk
	obj.db = _db
	setmetatable(obj, Table)
	return obj
end

function Table:first()
	return self:find():first()
end

function Table:last()
	return self:find():last()
end

function Table:each(func)
	return self:find():each(func)
end

function Table:find(...)
	return Query:new("SELECT * FROM " .. self.name, {}, self):parseArgs({...})
end

function Table:count(where)
	local result = Query:new("SELECT COUNT(1) as count FROM " .. self.name, {}, self):where(where):first().count
	return result
end

function Table:destroy(...)
	return Query:new("DELETE FROM " .. self.name, {}, self):parseArgs({...})
end

function Table:insert(data)
	assert(data)
	local dt = type(data)
	assert(dt == 'table')
	if #data == 0 then
		data = {data}
	end
	local sql = string.format("INSERT INTO %s (%s) VALUES\n", self.name, table.concat(get_keys(data[1]), ", "))
	local parameters = {}
	local values = {}

	-- insert placeholder text
	local placeholder_values = {}
	local seed = 0
	for k,v in pairs(data[1]) do
		table.insert(placeholder_values, self.db:placeholder(seed))
		seed = seed + 1
	end
	local placeholder_text = string.format('(%s)', table.concat(placeholder_values, ', '))

	for i,x in ipairs(data) do
		table.insert(values, placeholder_text)
		for a,b in ipairs(get_values(x)) do
			table.insert(parameters, b)
		end
	end

	sql = sql .. table.concat(values, ",\n")

	if (self.db.insertKludge) then
		sql = sql .. self.db.insertKludge()
	end
	return Query:new(sql, parameters, self):execute()
end

function Table:update(fields, where)
	assert(type(fields) == 'table')
	local parameters, f, seed = {},{},0
	for k,v in pairs(fields) do
		table.insert(f, k .. ' = ' .. self.db.placeholder(seed))
		seed = seed + 1
		table.insert(parameters, value)
	end
	local sql = string.format("UPATE %s SET %s", this.name, table.concat(f, ', '))
	return Query:new(sql, parameters, self):where(where)
end

Query.operationsMap = {}
Query.operationsMap['='] = '='
Query.operationsMap['!'] = '<>'
Query.operationsMap['>'] = '>'
Query.operationsMap['<'] = '<'
Query.operationsMap['>='] = '>='
Query.operationsMap['<='] = '<='
Query.operationsMap['!='] = '<>'
Query.operationsMap['<>'] = '<>'

function Query:new(sql, params, table)
	if params and type(params) ~= 'table' then params = {params} end
	local obj = {}
	setmetatable(obj, Query)
	obj.sql = sql
	obj.params = params or {}
	obj.table = table
	obj.db = table.db
	return obj
end

function Query:order(where)
	return self:append(' \nORDER BY %s', where)
end

function Query:limit(count, offset)
	if not offset then
		return self:append(' \nLIMIT %d', count)
	else
		return self:append(' \nLIMIT(%d,%d)', count, offset)
	end
end

function Query:raiseError(err)
	self.error = err
	return self
end

function Query:parseArgs(args)
	local at = type(args)
	if #args == 0 or at == 'function' or at == 'boolean' then return self end

	for i,v in ipairs(args) do
		local t = type(v)
		if t == 'number' or t == 'string' then
			local criteria = {}
			criteria[self.table.pk] = args[0]
			self:where(criteria)
		else
			local columns = v.columns or v
			local ct = type(columns)
			if ct == 'table' and #columns > 0 then
				self.sql = string.gsub(self.sql, "*", table.concat(columns, ', '))
			elseif ct == 'string' then
				self.sql = string.gsub(self.sql, "*", columns)
			end
			v.columns = nil

			local where = v.where or v
			local wt = type(where)
			if (wt == 'table' and #where == 0) then
				self:where(where)
			end
		end
	end

	return self
end

function Query:where(conditions)
	if not conditions then return self end

	local t = type(conditions)
	if t == 'number' then
		return self:append(' \nWHERE "%s" = %d', self.table.pk, conditions)
	elseif t == 'string' then
		return self:append(' \nWHERE "%s" = "%s"', self.table.pk, self.db.placeholder(#self.params))
	end

	local _conditions = {}
	for k,value in pairs(conditions) do
		local parts = string_split(string_trim(k), ' +')
		local property = parts[1]
		local operation = Query.operationsMap[parts[2]] or '='

		local vt = type(value)
		if vt == 'boolean' or vt == 'number' then
			table.insert(_conditions, string.format('"%s" %s %d', property, operation, value))
		elseif vt ~= 'table' then
			table.insert(self.params, value)
			table.insert(_conditions, string.format('"%s" %s %s', property, operation, self.db.placeholder(#self.params)))
		else
			local arrayConditions = {}
			for i,c in ipairs(value) do
				table.insert(self.params, c)
				table.insert(arrayConditions, self.db.placeholder(#self.params))
			end
			table.insert(_conditions,
				string.format('"%s" %s (%s)', property,
					operation == '<>' and 'NOT IN' or 'IN',
					table.concat(arrayConditions, ', ')
				)
			)
		end
	end
	return self:append(' \nWHERE ' .. table.concat(_conditions, ' \nAND'))
end

function Query:execute()
	return self.db:execute(self.sql, self.params)
end

function Query:each(func)
	assert(func)
	local call = self.db:execute(self.sql, self.params)
	when(call)
		:done(function(result)
			for i,v in ipairs(result) do
				func(v, i)
			end
		end)
	return call
end

function Query:first()
	return self:append(" LIMIT 1 "):execute()[1]
end

function Query:last()
	return self:append(" ORDER BY %s DESC LIMIT 1 ", this.table.pk):execute()[1]
end

function Query:append(...)
	local args = {...}
	if #args > 0 then
		self.sql = self.sql .. (#args == 1 and
			args[1] or
			string.format(unpack(args)))
	end
	return self
end

function SQLite:new(options)
	options = options or {}

	local obj = {
		dbType = 'SQLite',
		tables = {},
		sql = "",
		params = {},
		filename = options.filename
	}

	obj.tableSQL = [=[
SELECT name FROM sqlite_master
WHERE type='table'
ORDER BY name;
	]=]

	obj.db = obj.filename and
		sqlite3.open(obj.filename) or
		sqlite3.open_memory()

	setmetatable(obj, SQLite)

	return obj
end

function SQLite:translateType(typeName)
	if typeName == 'pk' then
		typeName = 'integer NOT NULL PRIMARY KEY AUTOINCREMENT'
	elseif typeName == 'money' then
		typeName = 'decmal'
	elseif typeName == 'date' then
		typeName = 'datetime'
	elseif typeName == 'string' then
		typeName = 'varchar(255)'
	end
	return typeName
end

function SQLite:placeholder(seed)
	return '?'
end

function SQLite:mapper(row, names)
	local obj = {}
	for i,v in names do
		obj[v] = row[i]
	end
	return obj
end

function SQLite:execute(sql, params)
	assert(sql and sql ~= '')
	params = params or {}
	local t
	for i,v in ipairs(params) do
		t = type(v)
		sql = string.gsub(sql, '?', (t == 'string' and "'" .. v .."'" or v), 1)
	end
	local data = {}
	for x in self.db:nrows(sql) do
		table.insert(data, x)
	end
	if self.db:errcode() ~= 0 then
		error(self.db:errmsg())
	end
	return data
end

function SQLite:run(sql, params)
	return Query:new(sql, params, { db = self})
end

function SQLite:loadTables()
	local _self = self
	local table_list = self:execute(self.tableSQL)
	local tables = {}
	for i,v in ipairs(table_list) do
		local info = self:execute('PRAGMA table_info(?)', {v.name})
		local t = { name = v.name}
		for i,row in ipairs(info) do
			if row.pk == 1 then
				t.pk = row.name
			end
		end
		table.insert(tables, t)
	end
	self.tables = {}
	for i,v in ipairs(tables) do
		local t = Table:new(v.name, v.pk, self)
		table.insert(self.tables, t)
		self[v.name] = t
	end
	return self
end

function SQLite:dropTable(tableName)
	return self:execute("DROP TABLE IF EXISTS " .. tableName + ";"):execute()
end

function SQLite:createTable(tableName, columns)
	local _sql = "CREATE TABLE " .. tableName .. "\n(\n"
	local _cols = {}

	local _pk
	for k,v in pairs(columns) do
		if v == 'pk' then _pk = k end
	end

	if not _pk then
		columns.id = 'pk'
		_pk = 'id'
	end

	for k,v in pairs(columns) do
		local colName, colParts, colType, translated, extras, declaration
		colName = k
		colParts = string_split(v, " ") or {v}
		colType = colParts[1]
		translated = SQLite:translateType(colType)
		extras = {}
		if #colParts > 2 then
			for i=3, #colParts do
				table.insert(extra, colParts[i])
			end
		end
		extras = table.concat(extras, ' ')
		declaration = string_trim(string_trim(colName) .. ' ' .. string_trim(translated) .. ' ' .. string_trim(extras))
		table.insert(_cols, declaration)
	end

	_sql = _sql .. table.concat(_cols, ',\n') .. "\n);"
	return Query:new(_sql, {}, Table:new(tableName, _pk, self)):execute()
end

Massive.Table = Table
Massive.Query = Query
Massive.SQLite = SQLite
