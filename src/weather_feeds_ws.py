from bs4 import BeautifulSoup
import pandas as pd
import urllib.request
import os
from datetime import date, timedelta 
today = date.today()

class WeatherDataFeed:
    """
    Loading Current Weather Data from Met.no

    Parameters
    ----------
    links : Locations
    symbol - Symbol of the Stock to extract the data
    
    Returns
    -------
    Integer
    DataFrame

    """
    def __init__(self):
        self.links = ['https://www.yr.no/place/Ireland/Leinster/Dublin/hour_by_hour_detailed.html',
                     'https://www.yr.no/place/Ireland/Munster/Cork/hour_by_hour_detailed.html']
        self.check_extention = ['.csv']
        self.sourceFolder = '/path/to/your/repo/weather_feeds/src'
        
    def get_weather_data_p0(self):
        for link in self.links:
            header=[]
            weather_data = []
            weather_data_th = []
            Url_Open = urllib.request.urlopen(link)
            soup = BeautifulSoup(Url_Open, 'html.parser')
            filename = link.replace('[^\w\s]','').replace('.html', '').replace('https://www.yr.no/place/Ireland/','').replace("/",'_')
            table = soup.find('table', attrs={'class':'yr-table yr-table-hourly-detailed yr-popup-area lp_hourly_detailed0'})
            
            # Weather Data Header
            table_header = table.find('thead')
            rows = table_header.find_all('tr')
            for row in rows:
                th = row.find_all('th')
                th = [head.text.strip() for head in th]
                header.append([head for head in th if head])
            header_columns=[]
            header_sub_columns=[]
            header_columns=header[0][:-1]
            header_sub_columns=header[1]
            header_columns.extend(header_sub_columns)
            
            # Weather Data Rows
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                weather_data.append([ele for ele in cols if ele])
            for row in rows:
                th = row.find_all('th')
                th = [timestamp.text.strip() for timestamp in th]
                weather_data_th.append([timestamp for timestamp in th if timestamp])
            
            # Build DataFrame
            if filename == 'Leinster_Dublin_hour_by_hour_detailed':
                dublin_data_df_0 = pd.DataFrame(weather_data[:])
                dublin_data_df_index_0 = pd.DataFrame(weather_data_th[:])
                dublin_weather_data_0=pd.concat([dublin_data_df_index_0,dublin_data_df_0],axis=1)
                dublin_weather_data_0.columns = header_columns[:]
                dublin_weather_data_0['Date'] = date.today()
                dublin_weather_data_0['Time'] = pd.to_datetime(dublin_weather_data_0['Time'], format='%H:%M').dt.time
                dublin_weather_data_0['Date_Time'] = pd.to_datetime(dublin_weather_data_0['Date'].apply(str) + ' ' + dublin_weather_data_0['Time'].apply(str))
                self.dublin_weather_data_0 = dublin_weather_data_0[['Date_Time','Weather','Temp.','Precipitation','Wind','Pressure','Humid-ity','Dew point','Total',
                                                           'Fog','Low clouds','Middle clouds','High clouds']]
            elif filename == 'Munster_Cork_hour_by_hour_detailed':
                cork_data_df_0 = pd.DataFrame(weather_data[:])
                cork_data_df_index_0 = pd.DataFrame(weather_data_th[:])
                cork_weather_data_0=pd.concat([cork_data_df_index_0,cork_data_df_0],axis=1)
                cork_weather_data_0.columns = header_columns[:]
                cork_weather_data_0['Date'] = date.today()
                cork_weather_data_0['Time'] = pd.to_datetime(cork_weather_data_0['Time'], format='%H:%M').dt.time
                cork_weather_data_0['Date_Time'] = pd.to_datetime(cork_weather_data_0['Date'].apply(str) + ' ' + cork_weather_data_0['Time'].apply(str))
                self.cork_weather_data_0 = cork_weather_data_0[['Date_Time','Weather','Temp.','Precipitation','Wind','Pressure','Humid-ity','Dew point','Total',
                                                           'Fog','Low clouds','Middle clouds','High clouds']]
            else:
                pass
        
    def get_weather_data_p1(self):
        for link in self.links:
            header=[]
            weather_data = []
            weather_data_th = []
            Url_Open = urllib.request.urlopen(link)
            soup = BeautifulSoup(Url_Open, 'html.parser')
            filename = link.replace('[^\w\s]','').replace('.html', '').replace('https://www.yr.no/place/Ireland/','').replace("/",'_')
            table = soup.find('table', attrs={'class':'yr-table yr-table-hourly-detailed yr-popup-area lp_hourly_detailed1'})
            # Weather Data Header
            table_header = table.find('thead')
            rows = table_header.find_all('tr')
            for row in rows:
                th = row.find_all('th')
                th = [head.text.strip() for head in th]
                header.append([head for head in th if head])
            header_columns=[]
            header_sub_columns=[]
            header_columns=header[0][:-1]
            header_sub_columns=header[1]
            header_columns.extend(header_sub_columns)
            # Weather Data Rows
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                weather_data.append([ele for ele in cols if ele])
            for row in rows:
                th = row.find_all('th')
                th = [timestamp.text.strip() for timestamp in th]
                weather_data_th.append([timestamp for timestamp in th if timestamp])
            # Build DataFrame
            if filename == 'Leinster_Dublin_hour_by_hour_detailed':
                dublin_data_df_1= pd.DataFrame(weather_data[:])
                dublin_data_df_index_1 = pd.DataFrame(weather_data_th[:])
                dublin_weather_data_1=pd.concat([dublin_data_df_index_1,dublin_data_df_1],axis=1)
                dublin_weather_data_1.columns = header_columns[:]
                dublin_weather_data_1['Date'] = date.today() + timedelta(days=1)
                dublin_weather_data_1['Time'] = pd.to_datetime(dublin_weather_data_1['Time'], format='%H:%M').dt.time
                dublin_weather_data_1['Date_Time'] = pd.to_datetime(dublin_weather_data_1['Date'].apply(str) + ' ' + dublin_weather_data_1['Time'].apply(str))
                self.dublin_weather_data_1 = dublin_weather_data_1[['Date_Time','Weather','Temp.','Precipitation','Wind','Pressure','Humid-ity','Dew point','Total',
                                                           'Fog','Low clouds','Middle clouds','High clouds']]
            elif filename == 'Munster_Cork_hour_by_hour_detailed':
                cork_data_df_1 = pd.DataFrame(weather_data[:])
                cork_data_df_index_1 = pd.DataFrame(weather_data_th[:])
                cork_weather_data_1=pd.concat([cork_data_df_index_1,cork_data_df_1],axis=1)
                cork_weather_data_1.columns = header_columns[:]
                cork_weather_data_1['Date'] = date.today() + timedelta(days=1)
                cork_weather_data_1['Time'] = pd.to_datetime(cork_weather_data_1['Time'], format='%H:%M').dt.time
                cork_weather_data_1['Date_Time'] = pd.to_datetime(cork_weather_data_1['Date'].apply(str) + ' ' + cork_weather_data_1['Time'].apply(str))
                self.cork_weather_data_1= cork_weather_data_1[['Date_Time','Weather','Temp.','Precipitation','Wind','Pressure','Humid-ity','Dew point','Total',
                                                           'Fog','Low clouds','Middle clouds','High clouds']]
            else:
                pass
        
    def get_weather_data_p2(self):
        for link in self.links:
            header=[]
            weather_data = []
            weather_data_th = []
            Url_Open = urllib.request.urlopen(link)
            soup = BeautifulSoup(Url_Open, 'html.parser')
            filename = link.replace('[^\w\s]','').replace('.html', '').replace('https://www.yr.no/place/Ireland/','').replace("/",'_')
            table = soup.find('table', attrs={'class':'yr-table yr-table-hourly-detailed yr-popup-area lp_hourly_detailed2'})
            # Weather Data Header
            table_header = table.find('thead')
            rows = table_header.find_all('tr')
            for row in rows:
                th = row.find_all('th')
                th = [head.text.strip() for head in th]
                header.append([head for head in th if head])
            header_columns=[]
            header_sub_columns=[]
            header_columns=header[0][:-1]
            header_sub_columns=header[1]
            header_columns.extend(header_sub_columns)
            # Weather Data Rows
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                weather_data.append([ele for ele in cols if ele])
            for row in rows:
                th = row.find_all('th')
                th = [timestamp.text.strip() for timestamp in th]
                weather_data_th.append([timestamp for timestamp in th if timestamp])
            # Build DataFrame
            if filename == 'Leinster_Dublin_hour_by_hour_detailed':
                dublin_data_df_2 = pd.DataFrame(weather_data[:])
                dublin_data_df_index_2 = pd.DataFrame(weather_data_th[:])
                dublin_weather_data_2=pd.concat([dublin_data_df_index_2,dublin_data_df_2],axis=1)
                dublin_weather_data_2.columns = header_columns[:]
                dublin_weather_data_2['Date'] = date.today() + timedelta(days=2)
                dublin_weather_data_2['Time'] = pd.to_datetime(dublin_weather_data_2['Time'], format='%H:%M').dt.time
                dublin_weather_data_2['Date_Time'] = pd.to_datetime(dublin_weather_data_2['Date'].apply(str) + ' ' + dublin_weather_data_2['Time'].apply(str))
                dublin_weather_data_2.rename(columns = {'Temp.' : 'Temperature','Humid-ity' : 'Humidity', 'Dew point' : 'Dew_point','Low clouds' : 'Low_clouds',
                              'Middle clouds' : 'Middle_clouds','High clouds' : 'High_clouds'}, inplace=True)
                self.dublin_weather_data_2 = dublin_weather_data_2[['Date_Time','Weather','Temperature','Precipitation','Wind','Pressure','Humidity','Dew_point','Total',
                                                           'Fog','Low_clouds','Middle_clouds','High_clouds']]
            elif filename == 'Munster_Cork_hour_by_hour_detailed':
                cork_data_df_2 = pd.DataFrame(weather_data[:])
                cork_data_df_index_2 = pd.DataFrame(weather_data_th[:])
                cork_weather_data_2=pd.concat([cork_data_df_index_2,cork_data_df_2],axis=1)
                cork_weather_data_2.columns = header_columns[:]
                cork_weather_data_2['Date'] = date.today() + timedelta(days=2)
                cork_weather_data_2['Time'] = pd.to_datetime(cork_weather_data_2['Time'], format='%H:%M').dt.time
                cork_weather_data_2['Date_Time'] = pd.to_datetime(cork_weather_data_2['Date'].apply(str) + ' ' + cork_weather_data_2['Time'].apply(str))
                cork_weather_data_2.rename(columns = {'Temp.' : 'Temperature','Humid-ity' : 'Humidity', 'Dew point' : 'Dew_point','Low clouds' : 'Low_clouds',
                              'Middle clouds' : 'Middle_clouds','High clouds' : 'High_clouds'}, inplace=True)
                self.cork_weather_data_2 = cork_weather_data_2[['Date_Time','Weather','Temperature','Precipitation','Wind','Pressure','Humidity','Dew_point','Total',
                                                           'Fog','Low_clouds','Middle_clouds','High_clouds']]
            else:
                pass
            
    def load_weather_data(self):
        dublin_weather=self.dublin_weather_data_0.append([self.dublin_weather_data_1,self.dublin_weather_data_2])
        # Run First time like this to include headers and second time you can use append mode and not include headers
        # dublin_weather.to_csv(os.path.join(os.getcwd(),"dublin_weather.csv"), index=False)
        dublin_weather.to_csv(os.path.join(os.getcwd(),"dublin_weather.csv"), mode='a', header=False, index=False)
        
        cork_weather=self.cork_weather_data_0.append([self.cork_weather_data_1,self.cork_weather_data_2])
        # Run First time like this to include headers and second time you can use append mode and not include headers
        # dublin_weather.to_csv(os.path.join(os.getcwd(),"dublin_weather.csv"), index=False)
        cork_weather.to_csv(os.path.join(os.getcwd(),"cork_weather.csv"), mode='a', header=False, index=False)
        
    def clean_up_weather_data(self):
        download_path = ''
        for filename in os.listdir(self.sourceFolder):
            if any(ext in filename for ext in self.check_extention):
                print(filename)
                download_path = (os.path.join(self.sourceFolder,filename))
                data = pd.read_csv(download_path)
                df = pd.DataFrame(data)
                # Sort the values in DB2 based on primary key
                df = df.sort_values("Date_Time", ascending = True)
                # De-duping the customers based on the DB primary key
                df = df.drop_duplicates(subset="Date_Time", keep="last")
                df.reset_index(drop=True, inplace=True)
                df.to_csv(download_path, index=False)
            else:
                pass

if __name__ == '__main__':
    build = WeatherDataFeed()
    build.get_weather_data_p0()
    build.get_weather_data_p1()
    build.get_weather_data_p2()
    dublin_weather=build.dublin_weather_data_0.append([build.dublin_weather_data_1,build.dublin_weather_data_2])
    cork_weather=build.cork_weather_data_0.append([build.cork_weather_data_1,build.cork_weather_data_2])
    dublin_weather.to_csv(os.path.join(os.getcwd(),"src/dublin_weather.csv"), header=True, index=False)
    cork_weather.to_csv(os.path.join(os.getcwd(),"src/cork_weather.csv"), header=True, index=False)
    # build.load_weather_data()
    build.clean_up_weather_data()