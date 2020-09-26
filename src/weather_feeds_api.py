from metno_locationforecast import Place, Forecast
import pandas as pd
import json
import os
os.environ['REPO'] = '/path/to/your/repo/weather_feeds/src'
os.chdir(os.environ['REPO'])

class WeatherDataFeed:
    '''
    Loading Current Weather Data from Met.no API

    Parameters
    ----------
    links : Locations
    symbol - Symbol of the Stock to extract the data
    
    Returns
    -------
    Integer
    DataFrame

    '''
    def __init__(self):
        self.USER_AGENT = 'metno_locationforecast/1.0'
        self.check_extention = ['.csv']
        self.sourceFolder = '/path/to/your/repo/weather_feeds/src'
        
    def get_weather_data(self, county='Dublin', latitude=53.33, longitude=-6.24, altitude=10):
        self.dublin = Place(county, latitude, longitude, altitude)
        self.forecast = Forecast(self.dublin, self.USER_AGENT, 'complete')
        self.forecast.update()
        df = pd.DataFrame()
        for interval in self.forecast.data.intervals:
            row = pd.DataFrame.from_dict(interval.variables,orient='index')
            row['start_time'] = interval.start_time
            row['end_time'] = interval.end_time
            df = df.append(row)
            
        df.columns = ['weather_feeds', 'start_time', 'end_time']
        df['weather_feeds'] = df['weather_feeds'].apply(str)
        df[['header','value']] = df.weather_feeds.str.split(expand=True)
        df['header'] = df['header'].str.replace(':', '')
        df2 = df.pivot(index = 'start_time', columns = 'header', values='value')
        self.df2 = df2
        
        
    def save_weather_data(self, file_name = 'default.csv'):
        weather_data = self.df2
        weather_data['Date_Time'] = weather_data.index
        weather_data = weather_data[['Date_Time',
                                   'air_pressure_at_sea_level',
                                   'air_temperature',
                                   'air_temperature_max',
                                   'air_temperature_min',
                                   'cloud_area_fraction',
                                   'cloud_area_fraction_high',
                                   'cloud_area_fraction_low',
                                   'cloud_area_fraction_medium',
                                   'dew_point_temperature',
                                   'fog_area_fraction',
                                   'precipitation_amount',
                                   'relative_humidity',
                                   'ultraviolet_index_clear_sky',
                                   'wind_from_direction',
                                   'wind_speed']]
        # Run First time like this to include headers and second time you can use append mode and not include headers
        # weather_data.to_csv(os.path.join(os.getcwd(),file_name), index = False)
        weather_data.to_csv(os.path.join(os.getcwd(),file_name), mode='a', header=False, index = False)  
        self.file_name = file_name
    
    def clean_up_weather_data(self):
        download_path = ''
        for filename in os.listdir(self.sourceFolder):
            if any(ext in filename for ext in self.check_extention):
                if filename == self.file_name:
                    print(filename)
                    download_path = (os.path.join(self.sourceFolder,filename))
                    data = pd.read_csv(download_path)
                    df = pd.DataFrame(data)
                    # Sort the values by Date Time
                    df = df.sort_values('Date_Time', ascending = True)
                    # De-duping the rows based on most up to date value
                    df = df.drop_duplicates(subset='Date_Time', keep='last')
                    df.to_csv(download_path, index = False)
                else:
                    pass
            else:
                pass

if __name__ == '__main__':
    weather = WeatherDataFeed()
    weather.get_weather_data(county='Dublin', latitude=53.33, longitude=-6.24, altitude=10)
    weather.save_weather_data(file_name = 'dublin.csv')
    weather.clean_up_weather_data()
    print('Dublin data is updated')
    
    weather.get_weather_data(county='Cork', latitude=51.90, longitude=-8.46, altitude=10)
    weather.save_weather_data(file_name = 'cork.csv')
    weather.clean_up_weather_data()
    print('Cork data is updated')
    db = weather.df2

