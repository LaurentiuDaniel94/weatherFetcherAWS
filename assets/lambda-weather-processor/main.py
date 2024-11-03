import os
import json
from urllib import request
from urllib.error import URLError
from dataclasses import dataclass
from datetime import datetime
from typing import Dict

@dataclass
class WeatherMessage:
    location: str
    timestamp: int
    temperature: float
    feels_like: float
    condition: str
    description: str
    wind_speed: float
    humidity: int
    coordinates: Dict[str, float]  # Added this field to match fetcher's message

    @classmethod
    def from_json(cls, json_str: str) -> 'WeatherMessage':
        return cls(**json.loads(json_str))

def get_weather_emoji(condition: str) -> str:
    emoji_map = {
        'Clear': '☀️',
        'Clouds': '☁️',
        'Rain': '🌧️',
        'Snow': '❄️',
        'Thunderstorm': '⛈️',
        'Drizzle': '🌦️',
        'Mist': '🌫️'
    }
    return emoji_map.get(condition, '🌡️')

def send_discord_message(notification: dict, webhook_url: str) -> None:
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
    
    req = request.Request(
        webhook_url,
        data=data,
        headers=headers,
        method='POST'
    )

    try:
        with request.urlopen(req) as response:
            if response.status != 204:
                print(f"Unexpected status code: {response.status}")
                return response.read().decode('utf-8')
    except URLError as e:
        print(f"Error sending to Discord: {str(e)}")
        raise

def handler(event, context):
    print(f"Received event: {json.dumps(event)}")  # Debug log
    
    try:
        DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK_URL']
        
        # Handle both direct invocation and SQS events
        records = []
        if isinstance(event, dict):
            if 'Records' in event:
                records = event['Records']
            elif 'body' in event:  # Handle direct test invocations
                records = [{'body': event['body']}]
            else:
                print(f"Unexpected event structure: {event}")
                return {
                    'statusCode': 400,
                    'body': 'Invalid event structure'
                }
                
        for record in records:
            try:
                print(f"Processing record: {json.dumps(record)}")  # Debug log
                
                if 'body' not in record:
                    print(f"No body in record: {record}")
                    continue
                
                message = WeatherMessage.from_json(record['body'])
                
                alerts = []
                if message.temperature > 30:
                    alerts.append(f"🌡️ High temperature alert: {message.temperature}°C")
                if message.temperature < 0:
                    alerts.append(f"❄️ Low temperature alert: {message.temperature}°C")
                if message.wind_speed > 20:
                    alerts.append(f"💨 High wind alert: {message.wind_speed} m/s")
                if message.humidity > 80:
                    alerts.append(f"💧 High humidity alert: {message.humidity}%")
                
                weather_emoji = get_weather_emoji(message.condition)
                notification = {
                    'location': message.location,
                    'time': datetime.fromtimestamp(message.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                    'condition': f"{weather_emoji} {message.condition}",
                    'alerts': alerts,
                    'details': (
                        f"Current temperature: {message.temperature}°C "
                        f"(feels like {message.feels_like}°C)\n"
                        f"Condition: {message.description}\n"
                        f"Wind speed: {message.wind_speed} m/s\n"
                        f"Humidity: {message.humidity}%\n"
                        f"Location: {message.coordinates['lat']}, {message.coordinates['lon']}"
                    )
                }
                
                send_discord_message(notification, DISCORD_WEBHOOK_URL)
                print(f"Sent Discord notification for {message.location}")
                
            except Exception as e:
                print(f"Error processing record: {str(e)}")
                print(f"Record content: {record}")  # Additional debug info
                continue
        
        return {
            'statusCode': 200,
            'body': 'Messages processed successfully'
        }
        
    except Exception as e:
        print(f"Error in handler: {str(e)}")
        raise