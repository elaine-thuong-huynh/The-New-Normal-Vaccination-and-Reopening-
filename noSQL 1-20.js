/*
Question 1-20 noSQL
Seminar Class: 9
Team: 3
*/

use "group_project" //our team sets name of the database to "group_project"

//Q1
db.country_vaccinations.aggregate([
    {$match: {country: "Singapore"}},
    {$project: {_id: 0, date: 1, "total_vaccinations": 1}}])

//Q2
db.country_vaccinations.aggregate([
    {$match: {country: {$in: ["Brunei", "Cambodia", "Indonesia", "Myanmar", "Laos", 
            "Malaysia", "Philippines", "Singapore", "Thailand", "Vietnam"]}} }, 
    {$group: {_id: {vaccines:"$vaccines", country: "$country"}, total: {$sum: {$convert:{input:"$daily_vaccinations", to: "int"}}}} },
    {$sort: {total: -1}},
    ])



//Q3
db.country_vaccinations.aggregate([
    {$project:{country:1,
        daily_vaccinations_per_million:{$convert:{input:"$daily_vaccinations_per_million",to: "int"}}
    }},
    {$group:{_id:{Country: "$country"}, max_daily_vaccinations_per_million:{$max: "$daily_vaccinations_per_million"}}},
     {$sort:{max_daily_vaccinations_per_million:-1}}
    ])



//Q4
db.country_vaccinations_by_manufacturer.aggregate([
    {$group: {_id:{VaccineName:"$vaccine"}, totalVaccine:{$sum:1}}},
    {$sort: {totalVaccine:-1}}
    ])



//Q5
db.country_vaccinations_by_manufacturer.aggregate([
    { $match: { 'location': 'Italy' } },
    { $group: { _id: ['no', '$vaccine'], dates: { $min: '$date' } } },
    { $project: { _id: 1, dates: { $toDate: "$dates" } } },
    { $sort: { vaccine: -1, dates: -1 } },
    { $group: { _id: '_id.no', dates: { $addToSet: '$dates' } } },
    {
        $project: {
            _id: 0, datesDiff: {
                $dateDiff: {
                    startDate: { $min: "$dates" },
                    endDate: { $max: "$dates" },
                    unit: "day",
                    //timezone: "+08:00"
                }
            }
        }
    }
])



//Q6
db.country_vaccinations_by_manufacturer.aggregate([
    { $group: { _id: { location: '$location' }, vaccines: { $addToSet: '$vaccine' } } },
    { $project: { _id: 1, count: { $size: "$vaccines" } } },
    { $sort: { count: - 1 } },
    { $limit: 1 }
])



//Q7
db.country_vaccinations.aggregate([
    {$project: {_id:0, country:1, vaccines:1, "people_fully_vaccinated_per_hundred":{$convert: {input:"$people_fully_vaccinated_per_hundred", to: "double"}}}},
    {$match: {"people_fully_vaccinated_per_hundred":{$gt:60}}},
    {$group: {_id:{country:"$country"}, vaccineType:{$addToSet:"$vaccines"}}},
    {$project:{country:1, "vaccineType":1}},
    {$sort: {"_id.country":1}},
    ])



//Q8
db.country_vaccinations_by_manufacturer.aggregate([{$match:{location:"United States"}},
    {$project:{date:{$convert:{input:"$date",to: "date"}}, vaccine:1,
        total_vaccinations:{$convert:{input:"$total_vaccinations",to: "int"}}
    }},
    {$group:{_id:{month:{$month : "$date"}, vaccine: "$vaccine"}, max_total_vaccinations:{$max: "$total_vaccinations"}}},
    {$sort:{_id:1}},
    ])



//Q9
db.country_vaccinations.aggregate([
         {$addFields: {tvph: { $convert: {input: "$total_vaccinations_per_hundred", to: "decimal"}}}},
         {$project:{country:1,date:1,tvph:1, fiftyormore:{$cond:[{$gte:["$tvph",50]},"$date","$$REMOVE"]}}},
         {$group:{
            _id:"$country",
            firstdate:{$min:{"$dateFromString":{dateString:"$date"}}},
            firstto50:{$min:{"$dateFromString":{dateString:"$fiftyormore"}}}
             }},
         {$project:{days:{$dateDiff:{startDate:"$firstdate",endDate:"$firstto50",unit:"day"}}}},
         {$match:{"days":{$nin:[null]}}}, 
         {$sort: {_id:1}}
        ])



//Q10
db.country_vaccinations_by_manufacturer.aggregate([
    {$group: {_id:{country: "$location", vaccine: "$vaccine"}, 
    max_vacc : {$max: {$convert: { input: "$total_vaccinations", to: "int"}}}
            }
    },
    {$group: {_id:{vaccine: "$_id.vaccine"}, total_vacc: {$sum: "$max_vacc"}}}, 
    {$sort: {total_vacc: -1}}])



//Q11
db.covid19data.distinct("population", {location: "Asia"})



//Q12
db.covid19data.aggregate([
    {$match: {location: {$in: ["Brunei", "Cambodia", "Indonesia", "Myanmar", "Laos", 
            "Malaysia", "Philippines", "Singapore", "Thailand", "Vietnam"]}}},
    {$group: {_id: "ASEAN", total: {$addToSet: {$sum: {$convert:{input: "$population", to: "decimal"}}}}}},
    {$project: {totalfinal:{$sum: "$total"}}},
    ])    



//Q13
db.country_vaccinations.aggregate([{$project: {source_name:1}},
{$group:{_id:{source_name: "$source_name"}}},
{$sort:{_id:1}}
])



//Q14
db.country_vaccinations.aggregate([
    {$match:{country:"Singapore"}},
    {$project: {_id:0, country:1, "date":{$convert:{input:"$date", to: "date"}}, "total_vaccinations": 1}},
    {$match:{date:{$gte:ISODate("2021-03-01"), $lte: ISODate("2021-05-21")}}},
    {$project:{_id:0, date:1, "total_vaccinations":1}}
    ])



//Q15
db.country_vaccinations.aggregate([
    { $match: { 'country': 'Singapore' } },
    { $group: { _id: '$country', date: { $addToSet: { $toDate: "$date" } } } },
    { $project: { _id: 1, first_date: { $min: "$date" } } },

])



//Q16
db.covid19data.aggregate([
    {
        $lookup: {
            from: "country_vaccinations",
            localField: "location",
            foreignField: "country",
            as: "coviddata"
        }
    },
    { $match: { 'location': "Singapore" } },
    { $project: { _id: '$location', dates: { $toDate: { $min: '$coviddata.date' } }, date_format: { $toDate: "$date" }, new_cases: 1 } },
    { $project: { _id: '$location', stda: { $cond: { if: { $gte: ['$date_format', '$dates'] }, then: "$date_format", else: 0 } }, new_cases: 1 } },
    { $match: { stda: { $ne: 0 } } },
    { $group: { _id: 'Singapore', total_set: { $push: { $toDecimal: '$new_cases' } } } },
    { $project: { _id: 1, total_new_cases: { $sum: '$total_set' } } }
])



//Q17
db.covid19data.aggregate([
    {$project:{_id:0, location: 1, date:{$convert:{input:"$date",to:"date"}}, "new_cases": {$convert: {input:"$new_cases", to: "double"}}}},
    {$match:{location:"Singapore"}},
    {$match:{date:{$lte: ISODate("2021-01-11")}}},
    {$group:{_id:{Country:"$location"}, newcases:{$sum:"$new_cases"}}},
    ])



//Q18
db.covid19data.createIndex({location: 1}) //create index to make the query execution time shorter

db.covid19data.aggregate([ {$lookup: {  from: "country_vaccinations_by_manufacturer",
                 let: {location_field: "$location", date_field: "$date"},
                 pipeline:[{ $match:{  $expr:{  $and: [ 
         { $eq: [ "$location",  "$$location_field" ] },
         { $eq: [ "$date", "$$date_field" ] }
     ] }
 }}],
                  as: "country_vaccinations_by_manufacturer" }},
           {$unwind: {path: "$country_vaccinations_by_manufacturer",
           preserveNullAndEmptyArrays: true}},
           {$match: {location: "Germany"}},
           {$project:{date:{$convert:{input:"$date",to: "date"}},
            total_vaccinations:{$convert:{input:"$country_vaccinations_by_manufacturer.total_vaccinations",to: "decimal"}},
            population:{$convert:{input:"$population",to: "decimal"}},
            new_cases_smoothed_per_million:{$convert:{input:"$new_cases_smoothed_per_million",to: "decimal"}},
            vaccine: { $ifNull:["$country_vaccinations_by_manufacturer.vaccine", "NIL"]}
        }
    }
    ,
    {$project:{date:1, vaccine:1, "Percentage of total vaccinations on each available vaccine in relation to its population/ %":{$round: [{ $divide: [ "$total_vaccinations", "$population" ] }, 7]},
    "Percentage of new cases/ %":{$round: [{ $multiply: [ {$divide: [{$multiply: ["$new_cases_smoothed_per_million", {$divide: ["$population", 1000000]}]},"$population"]}, 100 ] }, 7]}
    }}])



//Q19

//db.covid19data.createIndex({location: 1})
//Index is already created in Q18

db.covid19data.aggregate([   
        {$addFields:  { dateC: {$convert:{input: "$date", to: "date"}}}},
                {$lookup:
         {from: "country_vaccinations_by_manufacturer",
           let: { country: "$location", date_of_vaccination: "$dateC" },
           pipeline: [{ $match:{ $expr:{ $and:
                       [{ $eq: [ "$location",  "$$country" ] },
                        { $eq: [ {$convert:{input: "$date", to: "date"}}, {$dateAdd: { 
                             startDate:"$$date_of_vaccination" ,
                             unit: "day",
                             amount: 40,}}]}]}}},],
           as: "data_40"}
    },
        {$unwind: {path: "$data_40",
           preserveNullAndEmptyArrays: true}},
    {$match: {location: "Germany"}}, 
    {$addFields:  {tv: {$convert: {input: "$data_40.total_vaccinations", to: "int"}}}},
    {$setWindowFields: {
      sortBy: { dateC: 1, vaccine:1 },
      output: {
          Total_vaccinations_after_20_days: {$shift:{output: "$tv", by: -80}},
          Total_vaccinations_after_30_days: {$shift:{output: "$tv", by: -40}},
          Total_vaccinations_after_40_days: {$shift:{output: "$tv", by: 0}}
              }
          }
      }, 
           {$project: {_id:0, location:1, dateC:1, vaccine: { $ifNull:["$data_40.vaccine", "null"]},
           Total_vaccinations_after_20_days:1,  Total_vaccinations_after_30_days: 1, Total_vaccinations_after_40_days:1}}
    ])



//Q20 

//db.covid19data.createIndex({location: 1})
//Index is already created in Q18

db.country_vaccinations_by_manufacturer.aggregate([
    {$match: {location: "Germany"}},
    {$group: {_id:{location:"$location", date:{$convert:{input: "$date", to: "date"}}}, 
            total_accumulated_vaccinations: {$sum: {$convert: {input: "$total_vaccinations", to: "decimal"}}}}},
    {$lookup:
         { from: "covid19data",
           let: { country: "$_id.location", date_of_vaccination: "$_id.date" },
           pipeline: [{ $match:{ $expr:{ $and:
                       [{ $eq: [ "$location",  "$$country" ] },
                        { $eq: [ {$convert:{input: "$date", to: "date"}}, "$$date_of_vaccination" ] }]}}},],
           as: "data_0"}
    },
    {$lookup:
         { from: "covid19data",
           let: { country: "$_id.location", date_of_vaccination: "$_id.date" },
           pipeline: [{ $match:{ $expr:{ $and:
                       [{ $eq: [ "$location",  "$$country" ] },
                        { $eq: [ {$convert:{input: "$date", to: "date"}}, {$dateAdd: { 
                             startDate:"$$date_of_vaccination" ,
                             unit: "day",
                             amount: 21,}}]}]}}},],
           as: "data_21"}
    },
    {$lookup:
         {from: "covid19data",
           let: { country: "$_id.location", date_of_vaccination: "$_id.date" },
           pipeline: [{ $match:{ $expr:{ $and:
                       [{ $eq: [ "$location",  "$$country" ] },
                        { $eq: [ {$convert:{input: "$date", to: "date"}}, {$dateAdd: { 
                             startDate:"$$date_of_vaccination" ,
                             unit: "day",
                             amount: 60,}}]}]}}},],
           as: "data_60"}
    },
   {$lookup:
         {from: "covid19data",
          let: { country: "$_id.location", date_of_vaccination: "$_id.date" },
          pipeline: [{ $match:{ $expr:{ $and:
                       [{ $eq: [ "$location",  "$$country" ] },
                        { $eq: [ {$convert:{input: "$date", to: "date"}}, {$dateAdd: { 
                             startDate:"$$date_of_vaccination" ,
                             unit: "day",
                             amount: 120,}}]}]}}},],
            as: "data_120"}
    },
    {$unwind: {path: "$data_0", preserveNullAndEmptyArrays: true}},
    {$unwind: {path:"$data_21", preserveNullAndEmptyArrays: true}},
    {$unwind: {path:"$data_60", preserveNullAndEmptyArrays: true}},
    {$unwind: {path:"$data_120", preserveNullAndEmptyArrays: true}},
    {$project: {_id: 0, "total_accumulated_vaccinations":1,"data_0.date":1,"data_0.new_cases_smoothed":1, "data_21.date":1, "data_21.new_cases_smoothed":1, 
    rate_of_change_21 : {$round: [ {$multiply: [{$divide: [ {$subtract: [{$toDecimal: "$data_21.new_cases_smoothed"}, 
    {$toDecimal: "$data_0.new_cases_smoothed"}]}, {$toDecimal: "$data_0.new_cases_smoothed"}]}, 100]},3
                                ]
                        },
    "data_60.date":1, "data_60.new_cases_smoothed":1,
    rate_of_change_60 : {$round: [ {$multiply: [{$divide: [ {$subtract: [{$toDecimal: "$data_60.new_cases_smoothed"}, 
    {$toDecimal: "$data_0.new_cases_smoothed"}]}, {$toDecimal: "$data_0.new_cases_smoothed"}]}, 100]},3
                                ]
                        },
    "data_120.date":1, "data_120.new_cases_smoothed":1,
    rate_of_change_120 : {$round: [ {$multiply: [{$divide: [ {$subtract: [{$toDecimal: "$data_120.new_cases_smoothed"}, 
    {$toDecimal: "$data_0.new_cases_smoothed"}]}, {$toDecimal: "$data_0.new_cases_smoothed"}]}, 100]},3
                                ]
                        }
        
                }
    },
    {$sort: {"data_0.date" :1}}
   ])


