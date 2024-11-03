# processor_lambda/main.py
import os
import json
import boto3
from urllib import request
from urllib.error import URLError
from datetime import datetime
from typing import Dict, Any

class TimestreamWriter:
    def __init__(self):
        self.write_client = boto3.client('timestream-write')
        self.database = os.environ['TIMESTREAM_DATABASE']
        self.table = os.environ['TIMESTREAM_TABLE']

    def write_weather_data(self, weather_data: Dict[str, Any]):
        current_time = str(int(datetime.utcnow().timestamp() * 1000))

        # Prepare common dimensions
        common_dimensions = [
            {'Name': 'location', 'Value': weather_data['location']},
            {'Name': 'condition', 'Value': weather_data['condition']}
        ]

        # Prepare records
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
            print(f"Successfully wrote records to Timestream: {result}")
        except Exception as e:
            print(f"Error writing to Timestream: {str(e)}")
            raise

def send_discord_message(notification: dict, webhook_url: str) -> None:
    """Send formatted message to Discord"""
    embed = {
        "title": f"Weather Update for {notification['location']}",
        "description": notification['details'],
        "color": 3447003,  # Blue color
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

    # Add alert fields if any
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
    print(f"Received event: {json.dumps(event)}")
    timestream = TimestreamWriter()
    
    for record in event['Records']:
        try:
            # Parse message from SQS
            weather_data = json.loads(record['body'])
            
            # Write to Timestream
            timestream.write_weather_data(weather_data)
            print(f"Weather data written to Timestream for {weather_data['location']}")

            # Prepare alerts
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
                'condition': f"{weather_data['condition']}",
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
            continue
    
    return {
        'statusCode': 200,
        'body': 'Processing completed successfully'
    }