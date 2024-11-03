# fetcher_lambda/main.py
import os
import json
import boto3
from urllib import request, parse
from urllib.error import URLError
from datetime import datetime
from typing import Dict, Any

def get_weather_data(api_key: str, lat: float = 46.7712, lon: float = 23.6236) -> Dict[str, Any]:
    """Fetch weather data from OpenWeather API using urllib"""
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    
    # Build query parameters
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric"
    }
    
    # Create full URL with parameters
    url = f"{base_url}?{parse.urlencode(params)}"
    
    try:
        with request.urlopen(url) as response:
            return json.loads(response.read().decode('utf-8'))
    except URLError as e:
        print(f"Error fetching weather data: {str(e)}")
        raise

def format_weather_message(weather_data: Dict[str, Any]) -> Dict[str, Any]:
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
        # Get environment variables
        api_key = os.environ['WEATHER_API_KEY']
        queue_url = os.environ['QUEUE_URL']
        
        # Initialize AWS SQS client
        sqs = boto3.client('sqs')
        
        # Fetch weather data
        weather_data = get_weather_data(api_key)
        
        # Format the message
        message = format_weather_message(weather_data)
        
        # Send to SQS
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message),
            MessageGroupId="weather-updates"
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