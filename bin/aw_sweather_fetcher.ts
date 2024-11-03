#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { WeatherNotificationStack } from '../lib/weather-notification-stack';

const app = new cdk.App();
new WeatherNotificationStack(app, 'WeatherNotificationStack');

