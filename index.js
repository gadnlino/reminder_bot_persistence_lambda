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
                console.log("criando lembrete...")
                const putItemResp = await awsService.dynamodb
                    .putItem(remindersTableName, reminder);
            }
            else {
                console.log("deletando lembrete...")
                await awsService.dynamodb.deleteItem(remindersTableName, {
                    "uuid": reminder.uuid
                });
            }

            await manageRulesAndReminders();
        });
    });

    async function manageRulesAndReminders() {
        const now = new Date();
        
        const scanResp = await awsService.dynamodb.scan(remindersTableName);

        let reminders = scanResp.Items.map(item => ({
            uuid: item.uuid,
            reminder_date: item.reminder_date
        }));

        reminders.sort(function (a, b) {
            if (a.reminder_date <= b.reminder_date) return -1;
            return 1;
        });

        reminders = reminders.filter(r=>new Date(r.reminder_date) >= now);

        if (reminders.length > 20) {
            reminders = reminders.slice(0, 1);
        }

        const listRulesResp = await awsService.cloudWatchEvents
            .listRules("default", ruleNamePreffix);

        const rules = listRulesResp.Rules.map(rule => ({
            name: rule.Name,
            uuid: rule.Name.split("_")[2]
        }));

        //get the reminders that have associated rules with it
        const intersection = reminders
            .filter(r => rules.find(rule => rule.uuid === r.uuid) !== undefined);

        //if a reminder does not have an associated rule, i have to create it
        const remindersToAdd = reminders.filter(r =>
            intersection.find(rule => rule.uuid === r.uuid) === undefined);

        //if a rule does not have a reminder on dynamo(among those 20), then it must be removed
        const rulesToRemove = rules.filter(r =>
            intersection.find(reminder => reminder.uuid === r.uuid) === undefined);

        if (rulesToRemove.length > 0) {
            removeRules(rulesToRemove);
        }
        else console.log("There was no rules to remove");

        if (remindersToAdd.length > 0) {
            createRulesForReminders(remindersToAdd);
        }
        else console.log("there was no reminders to add");
    }

    function removeRules(rules) {

        console.log("rules to remove: ");
        console.log(`${JSON.stringify(rules)}`);

        rules.forEach(async rule => {
            const { name, uuid } = rule;

            await removeTargetsForRule(name);
            await removeInvokeLambdaPermission(remindersLambdaName, `${statementPreffix}${uuid}`);
            await awsService.cloudWatchEvents.deleteRule(name);
        });

        async function removeTargetsForRule(ruleName) {
            const listTargetsResp = await awsService
                .cloudWatchEvents.listTargets(ruleName);

            const targetIds = listTargetsResp.Targets.map(t => t.Id);

            if (targetIds.length > 0) {
                await awsService
                    .cloudWatchEvents.removeTargets(targetIds, ruleName);
            }
        }

        async function removeInvokeLambdaPermission(lambdaName, statementId) {
            const getPolicyResp = await awsService.lambda.getPolicy(lambdaName);

            const policy = JSON.parse(getPolicyResp.Policy);

            if (policy.Statement.find(s => s.Sid === statementId)) {
                const removePermResp = await awsService.lambda
                    .removePermission(lambdaName, statementId);
            }
        }
    }

    function createRulesForReminders(reminders) {

        console.log("reminders to add: ");
        console.log(`${JSON.stringify(reminders)}`);

        reminders.forEach(async reminder => {
            const { uuid, reminder_date } = reminder;
            const ruleName = `${ruleNamePreffix}${uuid}`;
            const putRuleResp = await createRule(ruleName, reminder_date);
            await putTargetsToRule(uuid, reminder.reminder_date, ruleName, putRuleResp.RuleArn);
        });

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

        async function putTargetsToRule(uuid,reminder_date, ruleName, ruleArn) {
            const targets = [{
                Arn: remindersLambdaArn,
                Id: `${targetPreffix}${uuid}`,
                Input: `{"uuid" : "${uuid}", \
                        "creation_date" : "${new Date().toISOString()}", \
                        "reminder_date":"${reminder_date}",\
                        "rule_arn" : "${ruleArn}", \
                        "rule_name" : "${ruleName}"}`
            }];

            const putTargetResp = await awsService
                .cloudWatchEvents.putTargets(ruleName, targets);
        }
    }
};