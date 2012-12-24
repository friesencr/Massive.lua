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
				if completed == total then
					if failed > 0 then
						deferred:reject(null_or_unpack(returns))
					else
						deferred:resolve(null_or_unpack(returns))
					end
				end
			end)
		else
			returns[i] = v
			completed = completed + 1
		end
	end
	return deferred
end

--
-- END LUA PROMISE
--

package.loadlib("lsqlite3.dll", "luaopen_lsqlite3")()

Massive = {}

local Table = {}
local Query = {}
local SQLite = {}

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
				temp = nil
			end
			break
		end
	end

	return temp
end

local function string_trim(s)
  return (s:gsub("^%s*(.-)%s*$", "%1"))
end

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

function Table:each()
	return self:find():each()
end

function Table:find(...)
	return Query("SELECT * FROM " .. self.name, {}, self):parseArgs({...})
end

function Table:count(where)
	return Query("SELECT COUNT(1) FROM " .. self.name, {}, self):where(where)
end

function Table:destroy(...)
	return Query("DELETE FROM " .. self.name, {}, self):parseArgs({...})
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
		table.insert(parameters, get_values(x))
	end

	sql = sql .. table.concat(values, ",\n")

	if (self.db.insertKludge) then
		sql = sql .. self.db.insertKludge()
	end
	return Query:new(sql, parameters, self)
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

function Query:new(sql, params, table)
	local obj = {}
	setmetatable(obj, Query)
	obj.sql = sql
	obj.params = params
	obj.table = table
	obj.db = table.db
end

function Query:order(where)
	return self._append(' \nORDER BY %s', where)
end

function Query:limit(count, offset)
	if not offset then
		return self:_append(' \nLIMIT %d', count)
	else
		return self._append(' \nLIMIT(%d,%d)', count, offset)
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
			if ct == 'table' then
				self.sql = self.sql.replace("*", table.concat(columns, ', '))
			elseif ct == 'string' then
				self.sql = self.sql.replace("*", columns)
			end
			arg.columns = nil

			local where = v.where or v
			local wt = type(where)
			if (not type == 'table' and #wt > 0) then
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
		return self._append(' \nWHERE "%s" = %d', self.table.pk, conditions)
	elseif t == 'string' then
		return self._append(' \nWHERE "%s" = %s', self.table.pk, self.db.placeholder(#self.params))
	end

	local _conditions = {}
	for k,v in pairs(conditions) do
		local parts = string_split(string_trim(key).split(' +'))
		local property = parts[1]
		local operation = parts[2] or '='

		local vt = type(value)
		if vt == 'boolean' or vt == 'number' then
			table.insert(_conditions, string.format('"%s" %s %d', property, operation, value))
		else
			if not vt == 'table' then
				table.insert(self.params, value)
				table.insert(_conditions, string.format('"%s" %s %s', property, operation, self.db.placeholder(#self.params)))
			else
				local arrayConditions = {}
				for i,v in ipairs(value) do
					table.insert(self.params, v)
					table.insert(arrayConditions, self.db.placeholder(#self.params))
				end
				table.insert(_conditions,
					string.format('"%s" %s (%s)', property,
						(operation == '!=' or operation == '<>') and 'NOT IN' or 'IN',
						table.concat(arrayConditions, ', ')
					)
				)
			end
		end
	end
	return self.append(' \nWHERE ' .. table.concat(_conditions, ' \nAND'))
end

function Query:execute()
	return self.db:execute(self.sql, self.params)
end

function Query:each(func)
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
	return self:append(" LIMIT(1) "):execute()
end

function Query:last()
	return self:append(" ORDER BY %s DESC LIMIT(1) ", this.table.pk):execute()
end

function Query:append(...)
	local args = {...}
	if #args > 0 then
		self.sql = self.sql .. #args == 1 and
			args[1] or
			string.format(unpack(args))
	end
end

(function()

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

	]=]

	obj.db = obj.filename and
		sqlite3.open(obj.filename) or
		sqlite3.open_membory()

	return obj
end

function SQLite:translateType(typeName)
	if typeName == 'pk' then
		typeName = 'INT NOT NULL PRIMARY KEY AUTO_INCREMENT'
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

function SQLite:execute(sql, params)
	for i,v in ipairs(params) do
		sql = string.gsub(sql, '?', v, 1)
	end
	return self.db:exec(sql)
end

function SQLite:run(sql, params)
	return Query:new(sql, params, self)
end

function SQLite:loadTables()
	local _self = self
	when(self:execute(self.tableSQL, {}))
		:done(function(result)
			for i,v in ipairs(result) do
				local t = Table:new(table.name, table.pk, _self)
				table.insert(_self.tables, t)
				self[t.name] = t
			end
		end)
	return self
end

function SQLite:dropTable(tableName)
	return self:execute("DROP TABLE IF EXISTS " .. tableName + ";")
end

end)()

Massive.Table = Table
Massive.Query = Query
Massive.SQLite = SQLite
