const awsService = require("./services/awsService.js");
const dotenv = require("dotenv");
dotenv.config();

exports.handler = async (event, context) => {

    const remindersTableName = process.env.REMINDERS_BOT_TABLE;
    const remindersLambdaName = process.env.REMINDERS_LAMBDA_NAME;
    const remindersLambdaArn = process.env.REMINDERS_LAMBDA_ARN;
    const ruleNamePreffix = "rule_reminder_";
    const statementPreffix = "reminder_statement_";
    const targetPreffix = "reminder_target_";

    async function removeRules(rules) {
        //TODO : FAZER EXATAMENTE O OPOSTO DA FUNCAO ABAIXO
    }

    async function createRulesForReminders(reminders) {
        reminders.forEach(async reminder => {
            const date = new Date(reminder.reminder_date);
            const ss = date.getUTCSeconds();
            const mm = date.getUTCMinutes();
            const hh = date.getUTCHours();
            const dd = date.getUTCDate();
            const MM = date.getUTCMonth();
            const yyyy = date.getUTCFullYear();

            const ruleName = `${ruleNamePreffix}${reminder.uuid}`;
            const scheduleExpression = `cron(${mm} ${hh} ${dd} ${MM + 1} ? ${yyyy})`;

            const putRuleResp = await awsService.cloudWatchEvents
                .putRule(ruleName, scheduleExpression, /*"reminder_bot_events"*/ null, "ENABLED");
            
            const action = "lambda:InvokeFunction";
            const functionName = remindersLambdaName;
            const principal = "events.amazonaws.com";
            const sourceArn = putRuleResp.RuleArn;
            const statementId = `${statementPreffix}${reminder.uuid}`;

            const addPermissionResp = await awsService.lambda
                .addPermission(action, functionName, principal, sourceArn, statementId);

            const targets = [{
                Arn: remindersLambdaArn,
                Id: `${targetPreffix}${reminder.uuid}`,
                Input: `{"uuid" : "${reminder.uuid}", \
                        "creation_date" : "${new Date().toISOString()}", \
                        "rule_arn" : "${putRuleResp.RuleArn}", \
                        "rule_name" : "${ruleName}"}`
            }];

            const putTargetResp = await awsService
                .cloudWatchEvents.putTargets(ruleName, targets);
        });
    }

    return new Promise((_, __) => {
        event.Records.forEach(async record => {
            const reminder = JSON.parse(record.body);

            const queryResp = await awsService.dynamodb.queryItems(
                remindersTableName,
                "#id = :value",
                { "#id": "uuid" },
                { ":value": reminder.uuid }
            );

            if (queryResp.Count === 0) {
                const putItemResp = await awsService.dynamodb
                    .putItem(remindersTableName, reminder);
            }

            const scanResp = await awsService.dynamodb.scan(remindersTableName);

            let reminders = scanResp.Items.map(item => ({
                uuid: item.uuid,
                reminder_date: item.reminder_date
            }));

            reminders.sort(function (a, b) {
                if (a.reminder_date <= b.reminder_date) return -1;
                return 1;
            });

            reminders = reminders.length < 40 ? reminders : reminders.slice(0, 39);

            const listRulesResp = await awsService.cloudWatchEvents
                .listRules("default", ruleNamePreffix);

            const rules = listRulesResp.Rules.map(rule => ({
                name: rule.Name,
                uuid: rule.Name.split("_")[2],
                arn: rule.Arn
            }));

            const intersection = reminders
                .filter(r => rules.find(rule => rule.uuid === r.uuid) !== undefined);

            const remindersToAdd = reminders.filter(r => 
                !intersection.find(rule => rule.uuid === r.uuid));

            const rulesToRemove = rules.filter(r => 
                !intersection.find(rule => rule.uuid === r.uuid));

            await removeRules(rulesToRemove);
            await createRulesForReminders(remindersToAdd);           
        });
    });
};