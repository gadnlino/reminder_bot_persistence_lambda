const awsService = require("./services/awsService.js");
/*const dotenv = require("dotenv");
dotenv.config();*/

exports.handler = async (event, context) => {

    const remindersTableName = process.env.REMINDERS_BOT_TABLE;
    const remindersLambdaName = process.env.REMINDERS_LAMBDA_NAME;
    const remindersLambdaArn = process.env.REMINDERS_LAMBDA_ARN;

    return new Promise((_, __) => {
        event.Records.forEach(async record => {
            const reminder = JSON.parse(record.body);

            const putItemResp = await awsService.dynamodb
                .putItem(remindersTableName, reminder);

            const date = new Date(reminder.reminder_date);
            const ss = date.getUTCSeconds();
            const mm = date.getUTCMinutes();
            const hh = date.getUTCHours();
            const dd = date.getUTCDate();
            const MM = date.getUTCMonth();
            const yyyy = date.getUTCFullYear();

            const ruleName = `rule_reminder_${reminder.uuid}`;
            const scheduleExpression = `cron(${mm} ${hh} ${dd} ${MM + 1} ? ${yyyy})`;
            const ruleState = "ENABLED";

            const putRuleResp = await awsService.cloudWatchEvents
                .putRule(ruleName, scheduleExpression, ruleState);

            const action = "lambda:InvokeFunction";
            const functionName = remindersLambdaName;
            const principal = "events.amazonaws.com";
            const sourceArn = putRuleResp.RuleArn;
            const statementId = `reminder_statement_${reminder.uuid}`;

            const addPermissionResp = await awsService.lambda
                .addPermission(action, functionName, principal, sourceArn, statementId);

            const targets = [{
                Arn: remindersLambdaArn,
                Id: `reminder_target_${reminder.uuid}`,
                Input: `{"uuid" : "${reminder.uuid}", \
                        "creation_date" : "${new Date().toISOString()}", \
                        "rule_arn" : "${putRuleResp.RuleArn}", \
                        "rule_name" : "${ruleName}"}`
            }];

            const putTargetResp = await awsService
                            .cloudWatchEvents.putTargets(ruleName, targets);
        });
    });
};