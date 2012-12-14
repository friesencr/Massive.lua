Query = {}

function Query:new(sql, params, table)
	local obj = {}
	Emitter:new(obj)
	setmetatable(obj, Query)
	obj.sql = sql
	obj.params = params
	obj.table = table
	obj.db = table.db
	obj:on('newListener', function(eventName)
		if eventName == 'row' then self:each() end
	end)
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

function Query:raise_error(err)
	self.error = err
	return self
end

function Query:parseArgs(...)
	local args = {...}
	if args.length == 0 or type(args[0]) == 'function' then return self end

	for i,v in ipairs(args) do
		local t = type(v)
		if t == 'number' or t == 'string' then
			local criteria = {}
			criteria[self.table.pk] = args[0]
			return self.where(criteria)
		end

		local columns = v.columns or v
		local ct = type(columns)
		if ct == 'table' then 
			self.sql = self.sql.replace("*", columns.join(','))
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

	return self;
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
		local parts = key.trim().split('') -- TODO
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
					table.insert(arrayConditions, self.db.placeholder(#self.params.length))
				end
				table.insert(_conditions, string.format('"%s" %s (%s)', property, 
					(operation == '!=' or operation == '<>') and 'NOT IN' or 'IN',
					arrayConditions:join(', '))) -- TODO
			end
		end
	end
	return self._append(' \nWHERE ' .. _conditions.join(' \nAND')) -- TODO
end

function Query:execute(callback)

end

function Query:each(callback)

end

function Query:first(callback)

end

function Query:last(callback)

end

function Query:_append(sql)

end
