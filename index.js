const uuid = require("uuid");
const AWS = require("aws-sdk");
const docClient = new AWS.DynamoDB.DocumentClient({region : "us-east-1"});

exports.handler = async (event, context) => {

    const tableName = process.env.REMINDERS_BOT_TABLE;    

    return new Promise((resolve,reject)=>{
        event.Records.forEach(record => {
            const { body } = record;
            
            const {username, data, assunto} = JSON.parse(body);
            
            const item = {
                uuid : uuid.v1(),
                username : username,
                creation_date : new Date().toISOString(),
                reminder_date : new Date(data).toISOString(),
                body : assunto,
                dismissed : false
            };

            const params = {
                TableName : tableName,
                Item : item
            };

            docClient.put(params, function(err, data) {
                if (err) {
                    reject(err);
                    //console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
                } else {
                    resolve(data);
                    //console.log("Added item:", JSON.stringify(data, null, 2));
                }
            });
        });
    });
};