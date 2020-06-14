//const region = process.env.AWS_REGION;
const region = "us-east-1";

const AWS = require("aws-sdk");
const sqs = new AWS.SQS({ region });
const docClient = new AWS.DynamoDB.DocumentClient({ region });
const cwevents = new AWS.CloudWatchEvents({ region });
const lambda = new AWS.Lambda({ region });

module.exports = {
    sqs: {
        sendMessage: async (queueURL, body) => {

            const params = {
                QueueUrl: queueURL,
                MessageBody: body
            };

            const req = sqs.sendMessage(params);

            return req.promise();
        },
        getMessages: async (queueURL) => {

            const params = {
                QueueUrl: queueURL
            };

            const req = sqs.receiveMessage(params);

            return req.promise();
        },

        deleteMessage: async (QueueUrl, ReceiptHandle) => {
            const params = {
                QueueUrl,
                ReceiptHandle
            };

            const req = sqs.deleteMessage(params);

            return req.promise();
        }
    },

    dynamodb: {
        putItem: async (tableName, item) => {

            const params = {
                TableName: tableName,
                Item: item
            };

            const req = docClient.put(params);

            return req.promise();
        },

        deleteItem : async (TableName, Key)=>{

            var params = {
                TableName, Key
            };

            const req = docClient.delete(params);

            return req.promise();
        },

        queryItems: async (TableName,
            FilterExpression,
            ExpressionAttributeNames,
            ExpressionAttributeValues) => {

            const params = {
                TableName,
                FilterExpression,
                ExpressionAttributeNames,
                ExpressionAttributeValues
            };

            const req = docClient.scan(params);

            return req.promise();
        },
        updateItem: async (TableName,
            Key,
            UpdateExpression,
            ExpressionAttributeValues,
            ReturnValues) => {

            const params = {
                TableName,
                Key,
                UpdateExpression,
                ExpressionAttributeValues,
                ReturnValues
            };

            const req = docClient.update(params);

            return req.promise();
        },
        scan: async (TableName) => {
            var params = {
                /*ExpressionAttributeNames: {
                 "#AT": "AlbumTitle", 
                 "#ST": "SongTitle"
                }, 
                ExpressionAttributeValues: {
                 ":a": {
                   S: "No One You Know"
                  }
                }, 
                FilterExpression: "Artist = :a", 
                ProjectionExpression: "#ST, #AT", */
                TableName
            };

            const req = docClient.scan(params);

            return req.promise();
        }
    },
    cloudWatchEvents: {
        putRule: async (Name, ScheduleExpression, State) => {

            const params = {
                Name,
                ScheduleExpression,
                State
            };

            const req = cwevents.putRule(params);

            return req.promise();
        },
        putTargets: async (Rule, Targets) => {

            const params = {
                Rule,
                Targets
            };

            const req = cwevents.putTargets(params);

            return req.promise();
        },

        listRules: async (EventBusName, NamePrefix) => {
            var params = {
                EventBusName,
                //Limit: 'NUMBER_VALUE',
                NamePrefix,
                //NextToken: 'STRING_VALUE'
            };

            const req = cwevents.listRules(params);

            return req.promise();
        },

        deleteRule: async (RuleName) => {
            var params = {
                Name: RuleName
            };

            const req = cwevents.deleteRule(params);

            return req.promise();
        },

        listTargets: async (RuleName) => {
            var params = {
                Rule: RuleName
            };

            const req = cwevents.listTargetsByRule(params);

            return req.promise();
        },

        removeTargets: async (TargetIds, RuleName) => {
            var params = {
                Ids: TargetIds,
                Rule: RuleName
            };

            const req = cwevents.removeTargets(params);

            return req.promise();
        }
    },
    lambda: {
        addPermission: async (Action, FunctionName, Principal, SourceArn, StatementId) => {

            var params = {
                Action,
                FunctionName,
                Principal,
                SourceArn,
                StatementId
            };

            const req = lambda.addPermission(params);

            return req.promise();
        },

        getPolicy : async (FunctionName)=>{
            var params = {FunctionName};

            const req = lambda.getPolicy(params);

            return req.promise();
        },

        removePermission: async (FunctionName, StatementId) => {
            var params = {
                FunctionName,
                StatementId,
                // Qualifier: 'STRING_VALUE',
                // RevisionId: 'STRING_VALUE'
            };

            const req = lambda.removePermission(params);

            return req.promise();
        }
    }
};