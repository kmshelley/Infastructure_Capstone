---
output: pdf_document
---
# SafeRoad
*Making the streets safer one city at a time.*

Katrina Adams, Katherine Shelley, Kasane Utsumi


## Section 1: Background [Adams]

### NYC Open Data and Vision Zero
*Background on Vision Zero and the NYC open data sets*

## Section 2: Data Sources [Shelley]
### Data collection process
*Discuss each data source and our data collection process*

### The "Data Grid"
*Describe our data grid and define each engineered feature*
Data Grid Dictionary

| Field Name | Definition     |
| :------------- | :------------- |
| 5mph | Percent of roads with a 5mph speed limit.|
| 15mph | Percent of roads with a 15mph speed limit.|
| 25mph | Percent of roads with a 25mph speed limit.|
| 35mph | Percent of roads with a 35mph speed limit.|
| 45mph | Percent of roads with a 45mph speed limit.|
| 55mph | Percent of roads with a 55mph speed limit.|
| 65mph | Percent of roads with a 65mph speed limit.|
| 85mph | Percent of roads with a 85mph speed limit.|
| bridges | Percent of all roads that is a bridge. |
| grid_Afternoon | Boolean value; 1 if the hour is between 14:00 and 17:59, 0 otherwise. |
| grid_Evening | Boolean value; 1 if the hour is between 18:00 and 21:59, 0 otherwise. |
| grid_Midday | Boolean value; 1 if the hour is between 10:00 and 13:59, 0 otherwise. |
| grid_Midnight | Boolean value; 1 if the hour is between 22:00 and 05:59, 0 otherwise. |
| grid_Morning | Boolean value; 1 if the hour is between 06:00 and 09:59, 0 otherwise. |
| grid_Weekday | Boolean value; 1 if the day of the week is a weekday, 0 otherwise. |
| grid_anyFatality | Boolean value; 1 if a collision resulted in a fatality, 0 otherwise. |
| grid_anyInjury | Boolean value; 1 if a collision resulted in an injury, 0 otherwise. |
| grid_collision_counter | Total count of collisions in this hour and zip code. |
| grid_cyclistFatalities | Total count of cyclist fatalities in this hour and zip code. |
| grid_cyclistInjuries | Total count of cyclist injuries in this hour and zip code. |
| grid_day | Day of the month. |
| grid_dayOfWeek | ISO day of the week; 1 = Monday, 7 = Sunday. |
| grid_fullDate | Full date and time stamp for this hour. |
| grid_hourOfDay | Hour of the day as an integer (0-23). |
| grid_id | Unique ID field for this hour and zip code entry. |
| grid_isAccident | Boolean value; 1 if a collision occurred in this hour and zip code, 0 otherwise. |
| grid_month | Month as an integer (1-12). |
| grid_motoristFatalities | Total count of motorist fatalities in this hour and zip code. |
| grid_motoristInjuries | Total count of motorist injuries in this hour and zip code. |
| grid_pedestrianFatalities | Total count of pedestrian fatalities in this hour and zip code. |
| grid_pedestrianInjuries | Total count of pedestrian injuries in this hour and zip code. |
| grid_totalFatalities | Total count of all fatalities in this hour and zip code. |
| grid_totalInjuries | Total count of all injuries in this hour and zip code. |
| grid_year | Year as a value (i.e. 2016) |
| grid_zipcode | Postal zip code. |
| liquor_licenses | Total number, per square mile, of liquor licenses active in this hour and zip code. |
| median_speed_limit | Median speed limit of all roads in this zip code. |
| road_cond_requests | Total number, per mile of road, of open road repair requests in this hour and zip code. |
| total_road_count | Total count of roads in this zip code. |
| total_road_length | Total length, in statute miles, of roads in this zip code. |
| tunnels | Percent of roads in this zip code that are listed as tunnels. |
| weather_Fog | Level of fog present in this hour and zip code. 0 = No fog, 1 = light fog, 2 = moderate fog, 3 = heavy fog. |
| weather_Fog_Dummy | Boolean value; 1 if fog is present in this hour and zip code, 0 otherwise. |
| weather_HourlyPrecip | Value, in inches, of rainfall. |
| weather_Rain | Level of rain present in this hour and zip code. 0 = No rain, 1 = light rain, 2 = moderate rain, 3 = heavy rain. |
| weather_Rain_Dummy | Boolean value; 1 if rain is present in this hour and zip code, 0 otherwise. |
| weather_SkyCondition | String representing cloud cover at different altitudes. |
| weather_SnowHailIce | Level of snow, hail, or ice present in this hour and zip code. 0 = No snow/hail/ice, 1 = light snow/hail/ice, 2 = moderate snow/hail/ice, 3 = heavy snow/hail/ice. |
| weather_SnowHailIce_Dummy | Boolean value; 1 if snow, hail, or ice is present in this hour and zip code, 0 otherwise. |
| weather_Visibility | Visibility in statute miles. |
| weather_WeatherType | tring representing inclement weather conditions. |
| weather_WetBulbFarenheit | Temperature, in degrees farenheit. |
| weather_WindSpeed | Windspeed in miles per hour. |
| zip_area | Area of this zip code in square miles. ||


## Section 3: SafeRoad Architecture

### Elasticsearch [Utsumi]
*Background on Elasticsearch and why we chose it for our data storage and retrieval system.*

### Spark [Shelley]
*Background on Spark and why we chose it for our data processing system.*

## Section 4: SafeRoad Process and Algorithm Development

### Automated system [Shelley]
*Describe our automated data collection and processing system.*

### Machine Learning Model [Adams]
*Describe our model (currently Random Forest), background about how it works, and why we chose it.*

## Section 5: User Interface

### Prediction Map [Utsumi]
*Images and description of the predictive part of our UI.*

### Exploratory Analysis [Shelley]
*Images and description of the EDA notebook.*

## Section 6: Conclusion

### Lessons Learned [All of us]

### Next Steps for SafeRoad [All of us]
