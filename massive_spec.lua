require 'massive'

describe("massive", function()

it ("should be able to create a database context", function()
	db = Massive.SQLite:new()
	assert.truthy(db)
end)

it ("should be able to split strings", function()
	local str = 'pizza is good'
	assert.equals(3, #Massive.Util.string_split(str, ' '))
end)

it("should be able to get tables automatically", function()
	db:createTable('derp', { name = 'string', rating = 'integer' })
	db:loadTables()
	assert.truthy(db.derp)
end)

it("should be able to insert data", function()
	db.derp:insert({ name = 'howdy', rating = 3 })
	db.derp:insert({ name = 'doody', rating = 6 })
	assert.equals(db.derp:count(), 2)
end)

it("should be able to filter records", function()
	local results = db.derp:find({name = 'howdy'}):execute()
	assert.equals(#results, 1)
	results = db.derp:find({rating = 3}):execute()
	assert.equals(#results, 1)
end)

end)
