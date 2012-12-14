local function get_keys(t)
	local keys = {}
	for k,v in pairs(t) do
		table.insert(keys, k)
	end
	return keys
end

Table = {}

function Table:new(tableName, pk, _db)
	self.name = tableName
	self.pk = pk
	self.db = _db
end

function Table:first(callback)
	self.find().first(callback)
end

function Table:last(callback)
	self.find().last(callback)
end

function Table:each(callback)
	self.find().each(callback)
end

function Table:find(arguments)
	return Query("SELECT * FROM " .. self.name, {}, self):parseArgs(arguments)
end

function Table:count(where)
	return Query("SELECT COUNT(1) FROM " .. self.name, {}, self):where(where)
end

function Table:destroy(arguments)
	return Query("DELETE FROM " .. self.name, {}, self):parseArgs(arguments)
end

function Table:insert(data)
	assert(data)
	local dt = type(data)
	if dt ~= 'table' then
		data = {data}
	end

	local sql = string.format("INSERT INTO %s (%s) VALUES\n", self.name, 
end
