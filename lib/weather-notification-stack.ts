import * as cdk from "aws-cdk-lib"
import * as lambda from "aws-cdk-lib/aws-lambda"
import * as sns from "aws-cdk-lib/aws-sns"
import * as sqs from "aws-cdk-lib/aws-sqs"
import * as events from "aws-cdk-lib/aws-events"
import * as targets from "aws-cdk-lib/aws-events-targets"
import * as ssm from "aws-cdk-lib/aws-ssm"
import * as path from "path"
import * as iam from "aws-cdk-lib/aws-iam"
import { Duration } from "aws-cdk-lib"
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources"


export class WeatherNotificationStack extends cdk.Stack {
  constructor(scope: cdk.App, id: string, props?: cdk.StackProps){
    super(scope, id, props);


    const openWeatherLambdaRole = new iam.Role(this, "OpenWeatherLambdaRole", {
        assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        managedPolicies: [
            iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
        ]
    })

    const deadLetterQueue = new sqs.Queue(this, "DeadLetterQueue", {
        queueName: "weather-dead-letter-queue",
        deliveryDelay: cdk.Duration.seconds(0),
        retentionPeriod: cdk.Duration.days(1)
    })

    const weatherQueue = new sqs.Queue(this, "WeatherQueue", {
        queueName: "weather-queue.fifo",
        deliveryDelay: cdk.Duration.seconds(0),
        contentBasedDeduplication: true,
        fifo: true,
        visibilityTimeout: cdk.Duration.seconds(300),
        retentionPeriod: cdk.Duration.hours(1),
        deadLetterQueue: {
            maxReceiveCount: 1,
            queue: deadLetterQueue
        }
    })

    openWeatherLambdaRole.addToPolicy(new iam.PolicyStatement({
        actions:["sqs:SendMessage"],
        resources: [weatherQueue.queueArn]
    }))

    const weatherTopic = new sns.Topic(this, "WeatherTopic", {
        topicName: "weather-topic",
        displayName: "Weather Topic",
        fifo: true,
        contentBasedDeduplication: true
    })
    
    const fetcherLambda = new lambda.Function(this, "FetcherWeatherLambda", {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "main.handler",
        timeout: cdk.Duration.seconds(30),
        code: lambda.Code.fromAsset(path.join(__dirname, '../assets/lambda-weather-fetcher')),
        role: openWeatherLambdaRole,
        environment: {
            QUEUE_URL: weatherQueue.queueUrl,
            WEATHER_API_KEY: ssm.StringParameter.valueForStringParameter(
                this, 
                "/weather-notification/api-key"
            )
        }
    });

    const processorLambdaRole = new iam.Role(this, "processorLambdaRole", {
        assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        managedPolicies: [
            iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
        ]
    });
    processorLambdaRole.addToPolicy(new iam.PolicyStatement({
        actions: ["sns:Publish"],
        resources: [weatherTopic.topicArn]
    }));

    const processorLambda = new lambda.Function(this, "ProcessorWeatherLambda", {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "main.handler",
        timeout: cdk.Duration.seconds(30),
        code: lambda.Code.fromAsset(path.join(__dirname, '../assets/lambda-weather-processor')),
        role: processorLambdaRole,
        environment: {
            TOPIC_ARN: weatherTopic.topicArn,
            DISCORD_WEBHOOK_URL: ssm.StringParameter.valueForStringParameter(this, "/weather-notification/discord-webhook-url"),
        }
    })

    processorLambda.addEventSource(new SqsEventSource(weatherQueue, {
        batchSize: 1
    })
    )

    const weatherFetchRule = new events.Rule(this, 'WeatherFetchRule', {
        schedule: events.Schedule.rate(Duration.hours(1)), // Runs every hour
        targets: [new targets.LambdaFunction(fetcherLambda)]
      });
}
    }
