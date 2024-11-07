from keboola.http_client import HttpClient

BASE_URL = "https://api.tomorrow.io/v4/"


class WeatherClient(HttpClient):
    def __init__(self, api_key: str):
        default_header = {"accept": "application/json"}

        super().__init__(base_url=BASE_URL,
                         default_http_header=default_header,
                         default_params={"apikey": api_key},
                         max_retries=3)

    def get_weather_forecast(self, location: str, units: str) -> list[dict]:
        endpoint = "weather/forecast"
        params = {
            "location": location,
            "units": units
        }

        response = self.get(endpoint_path=endpoint, params=params)

        output = []

        values = list(response['timelines'].values())[0]

        for val in values:
            response_dict = val["values"]
            response_dict["time"] = val["time"]
            response_dict["location"] = response["location"]["name"]

            output.append(response_dict)

        return output
