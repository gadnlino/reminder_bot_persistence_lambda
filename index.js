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

    function removeRules(rules) {

        async function removeTargetsForRule(ruleName) {
            const listTargetsResp = await awsService
                .cloudWatchEvents.listTargets(ruleName);

            const targetIds = listTargetsResp.Targets.map(t => t.Id);

            if (targetIds.length > 0) {
                await awsService
                    .cloudWatchEvents.removeTargets(targetIds, ruleName);
            }
        }

        async function removeInvokeLambdaPermission(lambdaName, statementId){
            const getPolicyResp = await awsService.lambda.getPolicy(lambdaName);

            const policy = JSON.parse(getPolicyResp.Policy);

            if(policy.Statement.find(s=>s.Sid === statementId)){
                await awsService.lambda
                .removePermission(lambdaName, statementId);
            }
        }

        rules.forEach(async rule => {
            const { name, uuid, arn } = rule;

            await removeTargetsForRule(name);
            await removeInvokeLambdaPermission(remindersLambdaName, `${statementPreffix}${uuid}`);
            await awsService.cloudWatchEvents.deleteRule(name);
        });
    }

    function createRulesForReminders(reminders) {

        async function createRule(ruleName, reminder_date) {

            const date = new Date(reminder_date);
            const ss = date.getUTCSeconds();
            const mm = date.getUTCMinutes();
            const hh = date.getUTCHours();
            const dd = date.getUTCDate();
            const MM = date.getUTCMonth();
            const yyyy = date.getUTCFullYear();

            const scheduleExpression = `cron(${mm} ${hh} ${dd} ${MM + 1} ? ${yyyy})`;

            const resp = await awsService.cloudWatchEvents
                .putRule(ruleName, scheduleExpression, "ENABLED");

            return resp;
        }

        async function addInvokeLambdaPermission(lambdaName, ruleArn, statementId) {
            const action = "lambda:InvokeFunction";
            const principal = "events.amazonaws.com";

            await awsService.lambda
                .addPermission(action, lambdaName, principal, ruleArn, statementId);
        }

        async function putTargetsToRule(uuid, ruleName, ruleArn) {
            const targets = [{
                Arn: remindersLambdaArn,
                Id: `${targetPreffix}${uuid}`,
                Input: `{"uuid" : "${uuid}", \
                        "creation_date" : "${new Date().toISOString()}", \
                        "rule_arn" : "${ruleArn}", \
                        "rule_name" : "${ruleName}"}`
            }];

            const putTargetResp = await awsService
                .cloudWatchEvents.putTargets(ruleName, targets);
        }

        reminders.forEach(async reminder => {
            const { uuid, reminder_date } = reminder;
            const ruleName = `${ruleNamePreffix}${uuid}`;
            const putRuleResp = await createRule(ruleName, reminder_date);
            await addInvokeLambdaPermission(remindersLambdaName,
                putRuleResp.RuleArn, `${statementPreffix}${uuid}`);

            await putTargetsToRule(uuid, ruleName, putRuleResp.RuleArn);
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

            removeRules(rulesToRemove);
            createRulesForReminders(remindersToAdd);
        });
    });
};