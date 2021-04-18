//Question 1.1
db.movies.aggregate([
	{$unwind: "$genres"},
	{$group: {_id: "$genres", avgDur:{$avg:"$runtime"}}},
	{$sort:{avgDur:-1}},{$limit:5}
])

//Question 1.2
db.movies.aggregate([
	{$match:{countries:"UK"}},
	{$unwind: "$countries"},
	{$group: { _id: "$countries", numberOfMovies: { $sum: 1 } }},
	{$match: {_id: {$ne:"UK"}, numberOfMovies: {$gte:10}}},
	{$project: { _id: 0, numberOfMovies: 1, country: "$_id"}}
]).pretty() 

//Question 2.1
//Run the two create index statements first individually to allow the fields to be searched.
db.movies.createIndex({plot:"text", title:"text"})
db.movies.createIndex({genres:1})
db.movies.aggregate([
	{$match:
	{$or: [
	{genres: {$in: ["Sport"]}}, 
	{$text:{$search:"Acrobatics Aerobics Gymnastics Archery Badminton Baseball Basketball Bicycle Motocross BMX Billiards Bobsleigh Bodybuilding Bowling Boxing Canoeing racing Cheerleading Chess Cricket Croquet Curling Dance Sport Darts Diving Dodgeball Fencing Figure Skating Football Soccer Frisbee Golf Handball Hockey Skating Ski Judo Karate Kayaking Kendo KickBox Boxing Kite Surfing Lacrosse Luge Martial Motocross Paintball Parachuting Paragliding Parkour Polo Powerlifting Rafting Gymnastics Climbing Rowing Rugby Sailing Sandboarding Diving Shooting Skateboarding Skiing Snowboarding Softball Speed Skating Sport Climbing Squash Sumo Wrestling Surfing Swimming Taekwondo Tennis Trampolining Triathlon Volleyball Windsurfing Wrestling"}} ]}}, 
	{$count: "moviesRelatedToSport"}
]).pretty()

//Question 2.2
db.movies.aggregate([
	{$match:
	{$or: [
		{genres: {$in: ["Sport"]}},
		{$text:{$search:"Acrobatics Aerobics Gymnastics Archery Badminton Baseball Basketball Bicycle Motocross BMX Billiards Bobsleigh Bodybuilding Bowling Boxing Canoeing racing Cheerleading Chess Cricket Croquet Curling Dance Sport Darts Diving Dodgeball Fencing Figure Skating Football Soccer Frisbee Golf Handball Hockey Skating Ski Judo Karate Kayaking Kendo KickBox Boxing Kite Surfing Lacrosse Luge martial Motocross Paintball Parachuting Paragliding Parkour Polo Powerlifting Rafting Gymnastics Climbing Rowing Rugby Sailing Sandboarding Diving Shooting Skateboarding Skiing Snowboarding Softball Speed Skating Sport Climbing Squash Sumo Wrestling Surfing Swimming Taekwondo Tennis Trampolining Triathlon Volleyball Windsurfing Wrestling"}} ]}},
	{$sort:{"imdb.rating":-1}}, {$limit:3},
	{$project: { _id:0, title:1, year:1, "imdb.rating":1, "imdb.votes":1}}
]).pretty()

//Question 3.1
//These statements need to be executed individually
db.movies.update({}, {$set: { "myRating": null}}, false, true)
db.movies.updateMany({'tomato.meter':{$exists:true}}, [{$set:{'tomato.average':{$divide:[{$add:['$tomato.meter',{$multiply:['$tomato.rating',10]},{$divide:['$tomato.fresh',4]},'$tomato.userMeter',{$multiply:['$tomato.userRating',20]}]},5]}}}])
db.movies.updateMany({$and:[{'tomato.average':{$exists:true}}, {imdb:{$exists:true}}, {metacritic:{$exists:true}}]}, [{$set:{'myRating':{$round:[{$divide:[{$add:['$tomato.average',{$multiply:['$imdb.rating',10]},'$metacritic']},3]},0]}}}])
db.movies.updateMany({$and:[{'tomato.average':{$exists:false}}, {imdb:{$exists:true}}, {metacritic:{$exists:true}}]}, [{$set:{'myRating':{$round:[{$divide:[{$add:['$tomato.average',{$multiply:['$imdb.rating',10]},'$metacritic']},2]},0]}}}])
db.movies.updateMany({$and:[{'tomato.average':{$exists:true}}, {imdb:{$exists:false}}, {metacritic:{$exists:true}}]}, [{$set:{'myRating':{$round:[{$divide:[{$add:['$tomato.average',{$multiply:['$imdb.rating',10]},'$metacritic']},2]},0]}}}])
db.movies.updateMany({$and:[{'tomato.average':{$exists:true}}, {imdb:{$exists:true}}, {metacritic:{$exists:false}}]}, [{$set:{'myRating':{$round:[{$divide:[{$add:['$tomato.average',{$multiply:['$imdb.rating',10]},'$metacritic']},2]},0]}}}])
db.movies.updateMany({$and:[{'tomato.average':{$exists:true}}, {imdb:{$exists:false}}, {metacritic:{$exists:false}},{"tomato.userReviews":{$gte: 10000}}]}, [{$set:{'myRating':{$round:['$tomato.average',0]}}}])
db.movies.updateMany({$and:[{'tomato.average':{$exists:false}}, {imdb:{$exists:true}}, {metacritic:{$exists:false}},{"imdb.votes":{$gte: 10000}}]}, [{$set:{'myRating':{$round:[{$multiply:['$imdb.rating',10]},0]}}}])
db.movies.updateMany({myRating:null}, [{$set:{'myRating':0}}])

//Question 3.2
db.movies.aggregate([
{$match:
	{$and:[
		{$or:[
			{$and: [
				{year:{$gte: 1980, $lt: 2000}},
				{$or:[
					{director:"Steven Spielberg"},
					{director:"Francis Ford Coppola"}
				]}
			]},
			{actors:"Christopher Lloyd"}
		]},
		{myRating: {$gte: 60}},
		{rated:"PG"}, 
		{'awards.wins':{$gt:5}}
	]
	}
},
{$project:{title:1, year:1, director:1, actors:1, "awards.wins":1, myRating:1,_id:0}}
]).pretty()


