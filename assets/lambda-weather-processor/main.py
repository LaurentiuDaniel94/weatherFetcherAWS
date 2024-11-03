# processor_lambda/main.py
import os
import json
from urllib import request
from urllib.error import URLError
from dataclasses import dataclass
from datetime import datetime

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
    """Send formatted message to Discord using urllib"""
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

    # Convert payload to bytes
    data = json.dumps(payload).encode('utf-8')
    
    # Create request
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
        # Send request
        with request.urlopen(req) as response:
            if response.status != 204:  # Discord returns 204 on success
                print(f"Unexpected status code: {response.status}")
                return response.read().decode('utf-8')
    except URLError as e:
        print(f"Error sending to Discord: {str(e)}")
        raise

def handler(event, context):
    DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK_URL']
    
    for record in event['Records']:
        try:
            # Parse message from SQS
            message = WeatherMessage.from_json(record['body'])
            
            # Check for alert conditions
            alerts = []
            if message.temperature > 30:
                alerts.append(f"🌡️ High temperature alert: {message.temperature}°C")
            if message.temperature < 0:
                alerts.append(f"❄️ Low temperature alert: {message.temperature}°C")
            if message.wind_speed > 20:
                alerts.append(f"💨 High wind alert: {message.wind_speed} m/s")
            if message.humidity > 80:
                alerts.append(f"💧 High humidity alert: {message.humidity}%")
            
            # Create notification
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
                    f"Humidity: {message.humidity}%"
                )
            }
            
            # Send directly to Discord
            send_discord_message(notification, DISCORD_WEBHOOK_URL)
            print(f"Sent Discord notification for {message.location}")
            
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            continue
    
    return {
        'statusCode': 200,
        'body': 'Messages processed successfully'
    }