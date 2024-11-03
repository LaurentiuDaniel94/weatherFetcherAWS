# fetcher_lambda/main.py
import os
import json
import boto3
import requests
from datetime import datetime
from typing import Dict, Any

# Cluj-Napoca coordinates
DEFAULT_LAT = 46.7712
DEFAULT_LON = 23.6236

def get_weather_data(api_key: str, lat: float = DEFAULT_LAT, lon: float = DEFAULT_LON) -> Dict[str, Any]:
    """Fetch weather data from OpenWeather API using coordinates"""
    url = f"https://api.openweathermap.org/data/2.5/weather"
    
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric"  # For Celsius
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching weather data: {str(e)}")
        raise

def format_weather_message(weather_data: Dict[str, Any]) -> Dict[str, Any]:
    """Format raw weather data into our message structure"""
    return {
        "location": weather_data["name"],
        "timestamp": int(datetime.now().timestamp()),
        "temperature": weather_data["main"]["temp"],
        "feels_like": weather_data["main"]["feels_like"],
        "condition": weather_data["weather"][0]["main"],
        "description": weather_data["weather"][0]["description"],
        "wind_speed": weather_data["wind"]["speed"],
        "humidity": weather_data["main"]["humidity"],
        "coordinates": {
            "lat": weather_data["coord"]["lat"],
            "lon": weather_data["coord"]["lon"]
        }
    }

def lambda_handler(event, context):
    try:
        # Initialize AWS SQS client
        sqs = boto3.client('sqs')
        
        # Fetch weather data using coordinates
        weather_data = get_weather_data(
            api_key=os.environ['WEATHER_API_KEY']
        )
        
        # Format the message
        message = format_weather_message(weather_data)
        
        # Send to SQS
        response = sqs.send_message(
            QueueUrl=os.environ['QUEUE_URL'],
            MessageBody=json.dumps(message),
            MessageGroupId="weather-updates"  # Required for FIFO queue
        )
        
        print(f"Weather data sent to queue: {message}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weather data successfully fetched and queued',
                'messageId': response['MessageId'],
                'data': message
            })
        }
        
    except Exception as e:
        print(f"Error in weather fetcher: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }