exports.up = function(knex) {
	return knex.schema.createTable('containerLogs', table => {
		table.string('containerId');
		table.integer('lastSentTimestamp');
	});
};

exports.down = function(_knex, Promise) {
	return Promise.reject(new Error('Not Implemented'));
};
