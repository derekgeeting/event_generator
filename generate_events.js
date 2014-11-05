var _ = require('lodash');
var async = require('async');
var chance = require('chance')();
var pg = require('pg');

var NUM_USERS = 100;
var startingEventTime = 1;
var endingEventTime = 270;
var maxUniqueEventsPerUserPerDay = 20;
var maxCountPerEventPerUserPerDay = 80;
var persist = true;

var site_id = '8a6a8d05-d458-3410-be3f-8cb69a9c63a6';
var app = 'Avention';

var eventTypes = [];
var users = [];
var userEvents = [];
var siteEvents = [];
var ueNum = 0;
var seNum = 0;

var db;
var dbDone;
var dbConnect = function(cb) {
	pg.connect('postgres://postgres@localhost/onesource_stats_1', function(err, client, done) {
		db = client;
		dbDone = done;
		cb(null);
	})
}

var getEventTypes = function(cb) {
	db.query('select id from events', function(err, result) {
		eventTypes = _.pluck(result.rows,'id');
		console.log('loaded '+eventTypes.length+' events');
		cb(null, eventTypes);
	});
}

var persistUser = function(user, cb) {
	db.query('insert into users (id, first_name, last_name, email, user_id, site_id) values ($1,$2,$3,$4,$5,$6)',
		[user.id,user.first_name,user.last_name,user.email,user.user_id,site_id], function(e, result) {
			cb(e, result);
		});
}

var persistUsers = function(cb) {
	if(persist) {
		console.log('persisting '+users.length+' users');
		async.eachLimit(users, 20, function(u, cb1) {
			persistUser(u, cb1);
		}, cb);
	} else {
		cb(null);
	}
}

var persistUserEvent = function(ue, cb) {
	if( (++ueNum) % 1000 === 0 ) {
		console.log('persisting userEvent #'+ueNum);
	}
	db.query('insert into user_event_stats (user_id, site_id, app, event_time_id, event_type, count) values ($1,$2,$3,$4,$5,$6)',
		[ue.user_id, site_id, app, ue.event_time_id, ue.event_type, ue.count], function(e, result) {
			cb(e, result);
		});
}

var persistUserEvents = function(cb) {
	if(persist) {
		console.log('persisting '+userEvents.length+' user events');
		async.eachLimit(userEvents, 20, function(ue, cb1) {
			persistUserEvent(ue, cb1);
		}, cb);
	} else {
		cb(null);
	}
}

var persistSiteEvent = function(se, cb) {
	if( (++seNum) % 1000 === 0 ) {
		console.log('persisting siteEvent #'+seNum);
	}
	db.query('insert into site_event_stats (site_id, app, event_time_id, event_type, count) values ($1,$2,$3,$4,$5)',
		[site_id, app, se.event_time_id, se.event_type, se.count], function(e, result) {
			cb(e, result);
		});
}

var persistSiteEvents = function(cb) {
	if(persist) {
		console.log('persisting '+siteEvents.length+' site events');
		async.eachLimit(siteEvents, 20, function(se, cb1) {
			persistSiteEvent(se, cb1);
		}, cb);
	} else {
		cb(null);
	}
}

var generateUsers = function(cb) {
	users = _.times(NUM_USERS, function() {
		var fn = chance.first();
		var ln = chance.last();
		return {
			id: chance.word({length:35}),
			first_name: fn,
			last_name: ln,
			email: chance.email(),
			user_id: fn+'.'+ln
		};
	});
	cb(null, users);
}

var generateEventsForDay = function(day, cb) {
	var usersForToday = _.sample(users, _.random(0, users.length));
	var siteEventsForToday = {};
	console.log('generateEventsForDay', day, usersForToday.length);
	_.each(usersForToday, function(u) {
		var eventsForUserForThisDay = _.sample(eventTypes, _.random(1, maxUniqueEventsPerUserPerDay));
		_.each(eventsForUserForThisDay, function(evt) {
			var count = _.random(1, maxCountPerEventPerUserPerDay)
			var ue = {
				user_id: u.id,
				event_time_id: day,
				event_type: evt,
				count: count
			};
			userEvents.push(ue);

			if(typeof siteEventsForToday[evt]==='undefined') {
				siteEventsForToday[evt] = 0;
			}
			siteEventsForToday[evt] += count;
		});
	});

	_.each(_.keys(siteEventsForToday), function(evt) {
		siteEvents.push({
			event_time_id: day,
			event_type: evt,
			count: siteEventsForToday[evt]
		})
	});
	//now do site events
	cb(null);
}

var generateEvents = function(cb) {
	var numDays = endingEventTime - startingEventTime + 1;
	async.times(numDays, function(n, next) {
		generateEventsForDay(n+1, next);
	}, cb);
}

async.series([
	dbConnect,
	getEventTypes,
	generateUsers,
	generateEvents,
	persistUsers,
	persistUserEvents,
	persistSiteEvents
], function(err, results) {
	if(err) {
		console.log('err', err);
	} else {
		console.log('done');
		_.each(userEvents, function(e) {
			//console.log(e.user_id+"\t"+e.event_time_id+"\t"+e.count+"\t"+e.event_type);
		});
		_.each(siteEvents, function(e) {
			//console.log(e.event_time_id+"\t"+e.count+"\t"+e.event_type);
		});
		dbDone();
	}
});