# processor_lambda/main.py
import os
import json
import boto3
from urllib import request
from urllib.error import URLError
from datetime import datetime

class TimestreamWriter:
    def __init__(self):
        self.write_client = boto3.client('timestream-write')
        self.database = os.environ['TIMESTREAM_DATABASE']
        self.table = os.environ['TIMESTREAM_TABLE']

    def write_records(self, weather_data: dict):
        """Write weather data to Timestream"""
        current_time = str(int(datetime.utcnow().timestamp() * 1000))

        # Common dimensions for all records
        common_dimensions = [
            {'Name': 'location', 'Value': weather_data['location']},
            {'Name': 'condition', 'Value': weather_data['condition']},
            {'Name': 'description', 'Value': weather_data['description']}
        ]

        # Create records for different measurements
        records = [
            {
                'Dimensions': common_dimensions,
                'MeasureName': 'temperature',
                'MeasureValue': str(weather_data['temperature']),
                'MeasureValueType': 'DOUBLE',
                'Time': current_time
            },
            {
                'Dimensions': common_dimensions,
                'MeasureName': 'feels_like',
                'MeasureValue': str(weather_data['feels_like']),
                'MeasureValueType': 'DOUBLE',
                'Time': current_time
            },
            {
                'Dimensions': common_dimensions,
                'MeasureName': 'humidity',
                'MeasureValue': str(weather_data['humidity']),
                'MeasureValueType': 'DOUBLE',
                'Time': current_time
            },
            {
                'Dimensions': common_dimensions,
                'MeasureName': 'wind_speed',
                'MeasureValue': str(weather_data['wind_speed']),
                'MeasureValueType': 'DOUBLE',
                'Time': current_time
            }
        ]

        try:
            result = self.write_client.write_records(
                DatabaseName=self.database,
                TableName=self.table,
                Records=records
            )
            print(f"Successfully wrote records to Timestream for {weather_data['location']}")
            return result
        except self.write_client.exceptions.RejectedRecordsException as e:
            print(f"Rejected records: {e}")
            for rr in e.response["RejectedRecords"]:
                print(f"Rejected record: {rr}")
            raise
        except Exception as e:
            print(f"Error writing to Timestream: {str(e)}")
            raise

def send_discord_message(notification: dict, webhook_url: str):
    """Send weather notification to Discord"""
    embed = {
        "title": f"Weather Update for {notification['location']}",
        "description": notification['details'],
        "color": 3447003,
        "fields": [
            {
                "name": "Condition",
                "value": notification['condition'],
                "inline": True
            }
        ],
        "footer": {
            "text": f"Time: {notification['time']}"
        }
    }

    for alert in notification['alerts']:
        embed["fields"].append({
            "name": "Alert",
            "value": alert,
            "inline": False
        })

    payload = {
        "embeds": [embed]
    }

    data = json.dumps(payload).encode('utf-8')
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Weather Bot'
    }
    
    req = request.Request(webhook_url, data=data, headers=headers, method='POST')

    try:
        with request.urlopen(req) as response:
            if response.status != 204:
                print(f"Unexpected Discord status code: {response.status}")
    except URLError as e:
        print(f"Error sending to Discord: {str(e)}")
        raise

def handler(event, context):
    print(f"Processing event: {event}")
    timestream = TimestreamWriter()

    for record in event['Records']:
        try:
            # Parse weather data from SQS message
            weather_data = json.loads(record['body'])
            print(f"Processing weather data: {weather_data}")

            # Write to Timestream
            timestream.write_records(weather_data)

            # Prepare alerts for Discord
            alerts = []
            if weather_data['temperature'] > 30:
                alerts.append(f"🌡️ High temperature alert: {weather_data['temperature']}°C")
            if weather_data['temperature'] < 0:
                alerts.append(f"❄️ Low temperature alert: {weather_data['temperature']}°C")
            if weather_data['wind_speed'] > 20:
                alerts.append(f"💨 High wind alert: {weather_data['wind_speed']} m/s")
            if weather_data['humidity'] > 80:
                alerts.append(f"💧 High humidity alert: {weather_data['humidity']}%")

            # Prepare Discord notification
            notification = {
                'location': weather_data['location'],
                'time': datetime.fromtimestamp(weather_data['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                'condition': weather_data['condition'],
                'alerts': alerts,
                'details': (
                    f"Current temperature: {weather_data['temperature']}°C "
                    f"(feels like {weather_data['feels_like']}°C)\n"
                    f"Condition: {weather_data['description']}\n"
                    f"Wind speed: {weather_data['wind_speed']} m/s\n"
                    f"Humidity: {weather_data['humidity']}%"
                )
            }

            # Send to Discord
            send_discord_message(notification, os.environ['DISCORD_WEBHOOK_URL'])
            print(f"Discord notification sent for {weather_data['location']}")

        except Exception as e:
            print(f"Error processing record: {str(e)}")
            print(f"Record content: {record}")
            continue

    return {
        'statusCode': 200,
        'body': 'Successfully processed records'
    }