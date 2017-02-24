const AWS = require('aws-sdk');
const request = require('request');
const _ = require('lodash');
const BigQuery = require('@google-cloud/bigquery');

const bigquery = BigQuery();

Date.prototype.yyyymmdd = function() {
  var mm = this.getMonth() + 1; // getMonth() is zero-based
  var dd = this.getDate();
  return [this.getFullYear(),
          (mm>9 ? '' : '0') + mm,
          (dd>9 ? '' : '0') + dd
         ].join('');
};

Date.prototype.addDays = function(days)
{
    var dat = new Date(this.valueOf());
    dat.setDate(dat.getDate() + days);
    return dat;
}

const doRegion = function(startTime, endTime) { return function(region) {
	const ec2 = new AWS.EC2({
		region: region
	});
	queryAndInsert(ec2, region, startTime, endTime, {
        StartTime: startTime.toISOString(),
        EndTime: endTime.toISOString()
    }, null);
}}

const queryAndInsert = function(ec2, region, startTime, endTime, params, nextToken) {
	const query = _.assign(params, {NextToken: nextToken});

	const date_str = startTime.yyyymmdd();
	const bqTable = bigquery.dataset('aws_cost').table('spot_price_history$' + date_str);
    ec2.describeSpotPriceHistory(query, function(err, data) {
        if (err) {
            console.log(err, err.stack);
        } else {
			if (data['NextToken']) {
				queryAndInsert(ec2, region, startTime, endTime, params, data['NextToken']);
			}
			rows = data['SpotPriceHistory'].map((x) =>
				_.assign(x, {'Region': region}))
                .filter((x) =>
                    (startTime <= x['Timestamp']) && (x['Timestamp'] <= endTime));
			bqTable.insert(rows)
				.then((data) => {
					console.log(data[0], data[1])
				});
        }
    });
}

exports.run = function() { //event, context) {
	//console.log(event);
	const date = process.env.DATE; //event['date'];
	const startTime = new Date(date);
	const endTime = new Date(date).addDays(1)

    new AWS.EC2({
        region: 'us-east-1'
    }).describeRegions({}, function(err, data) {
        if (err) {
            console.log(err, err.stack);
        } else {
            _.map(data['Regions'], 'RegionName').map(doRegion(startTime, endTime));
        }
    });
}

exports.run();
