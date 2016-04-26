# SafeRoad
*A Data-Driven Approach to Safer City Streets*

Katrina Adams, Katherine Shelley, Kasane Utsumi


## Section 1: Background

### Problem Definition and Context
According to the Centers for Disease Control 2014 Final Report, traffic fatalities are the leading cause of death for persons unser age 35 and the third leading cause of unintentional death amongst those under age 55. Regardless of prevalence, we believe that all traffic accidents are avoidable; which means no traffic fatality is acceptable.

There are several programs accross major US cities aimed at using data-based decisions to design safer streets and educate the public to reduce traffic injuries and fatalities. With our product, however, we look to fill a tactical gap, providing near-real-time prediction of traffic collisions that will show public leaders and traffic safety specialists when and where serious collisions are more likely to occur.
Our tool, SafeRoad, is a Big Data system for large-scale collision data mining and prediction. Our tool will identify what road and other conditions are important causal factors for fatal and serious injury collisions to develop long-term, city-specific strategies to meet the goal of zero traffic fatalities.

## Section 2: Data Sources

### Open Data
SafeRoad is built entirely on open and freely available data sets. In order to model the complexity of automobile collisions in a large metropolitan city street system, several datasets are collected and used as input to the SafeRoad model.

We identify collisions using historical automobile collision data retrieved from the New York City [Vision Zero](http://www.visionzeroinitiative.com/) project. The collisions data set contains geocoded records for all automobile collisions reported to the New York Police Department dating back to July, 2012.  Additional data are collected to enhance the accuracy of the model, such as weather retrieved from the [National Oceanic and Atmospheric Administration (NOAA)](http://www.noaa.gov/), and other city and state-specific data sets made available through the [New York State](https://data.ny.gov/) and [New York City](https://nycopendata.socrata.com/) open data initiatives. Below is a list of the data sources used in the SafeRoad model and user interface:

* Data sets:
	* New York City Collisions
	* NOAA hourly weather observations
	* Weather Underground hourly forecasts
	* New York City street segments
	* New York State 2013 Average Annualized Daily Traffic Counts
	* New York City 311 road condition reports
	* New York State issued liquor licenses
	* US Census Zip Code Tabulation Areas
	* NYPD Precinct areas
	* New York City City Council districts
	* New York City School districts

Each set of data is stored in a separate Elasticsearch index. Locational fields are mapped as Elasticsearch geo-shape point or polygon types for fast geospatial queries. For more information about Elasticsearch see section (XX).

### The Data Grid

The disparate data sets listed above are automatically collected on a daily basis and merged into a superset "grid" of data points to be used for training the predictive model. Each grid point represents a New York City zip code and local hour from the start of the Vision Zero collisions data set (July, 2012) to the most recent Vision Zero collision record.

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
| Median AADT | Median 2013 Annualized Average Traffic counts of streets in the zip code. |
| Average AADT | Mean 2013 Annualized Average Traffic counts of streets in the zip code. |
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

### Elasticsearch
We chose ElasticSearch as a data storage because of its fast search performance, the wide array support for advanced queries such as geo queries and aggregations, and the ease of scaling up/down the cluster. Kibana has been integral to our fast-paced product and feature development as we were able to quickly explore the data using "Discover" and data visualization features.

### Spark
We chose to build our data processing and analysis layer on top of a [Spark](http://spark.apache.org/) cluster. Spark is a cluster computing system where data are processed entirely in memory with MapReduce-style transformations through a data abstraction known as a Resilient Distributed Dataset (RDD). We chose Spark because of it's speed, flexibility, built-in machine learning library, MLLib, and existing Elasticsearch plugins.

## Section 4: SafeRoad Process and Algorithm Development

### Automated system
The SafeRoad Automated System is a daily process of data collection, storage, data cleaning, feature engineering, model training, and prediction.  On a daily basis the data are collected from the various sources listed above and stored in separate Elasticsearch indices. Once collected, data are combined in a "grid" where each row represents a zip code and hour in time. The data grid is built utilizing the speed of Spark transformations and Elasticsearch queries.

In a similar process a prediction grid containing the same data features as the data grid for the proceeding ten days, sans collision, is created using a combination of existing data sources and weather forecast data from [Weather Underground](https://www.wunderground.com/). The prediction grid is limited to ten days in the future due to the availability of weather forecast data.

The data grid is used to train the predictive model, after which predictions are updated based on the prediction grid.

### Machine Learning Model
SafeRoad uses a [Random Forest](https://www.stat.berkeley.edu/~breiman/RandomForests/cc_home.htm) model to predict the probability of a serious collision occurring in a given New York City zip code and hour; those probabilities are then averaged across a 24 hour period. Custom time periods can also be analyzed based on user input.

A serious collision is defined as a collision that resulted in at least one injury or fatality. The Vision Zero data set of historical collisions also allows for a more specific model predicting the probability of collisions affecting pedestrians, cyclists, and motorists.

A Random Forest is a machine learning algorithm that builds a series of decision trees, each with a randomly selected subset fields from the training, or truth data; data that has a label of the true outcome that is being predicted. Each decision tree builds rules that define whether or not an outcome will occur based on the data fields available to that particular tree, and the true outcomes available in the training data. To make a prediction, data is sent through each individual decision tree and a classification of the outcome is made. Each tree then "votes" on the outcome and a majority classification becomes the ultimate prediction. In our model we output not just a single classification, but a probability of that classification by averaging the number of positive outcomes, i.e. serious collision, by the number of trees in the forest. For example, if our model uses 100 decision trees and 75 of them predict that in a particular zip code and hour a serious collision will occur, then the probability is 75/100 or 75%.

Our model is trained using data from the start of our data grid, up to 90 days prior to the latest timestamp available. As serious as traffic collisions are, in our entire data grid the occurrence of a collision that results in injury or death is still quite rare when considering our data grid of every New York City zip code and hour for almost 5 years. All-in-all the data have approximately 2% postive outcomes (a serious collision occurred) and 98% negative outcomes. This skewed truth data does not lend itself well to training a robust machine learning model, so we built a smaller training set from the data grid containing the positive records, and a similar sized set of negative records that have been randomly sampled, with replacement. Thus, for training our model we have a data set that is approximately evenly weighted. The remaining 90 days of data are used for validating the model and assessing various accuracy scores. All validation of the model is done with the full set of data from the previous 90 days, and is hence weighted realistically.

### Feature Importance
A natural output of a decision tree or Random Forest model is a metric known as **feature importance**. There are various algorithms for determining feature importance, but generally speaking a feature’s importance is measured by averaging the difference in error caused by permuting the values of the feature in the training set on each tree. A feature importance rating is the normalized value (divided by the standard deviation) of the average change in the error. We provide the feature importance rating of each model generated as a tool for identifiying key factors in serious collisions.


## Section 5: User Interface

### Web UI

##### Prediction Map

For the SafeRoad front-end we created an interactive webpage built with NodeJS, Leaflet, and D3. The User Interface displays the SafeRoad model predictions in an interactive heat map and various charts. We also display the model diagnostics via Reciever Operator Curves (ROC) for each prediction.

![Prediction Map](./images/predictionMap.png?raw=true "Prediction Map")

##### Prediction Map - Zipcode View

![Prediction Map with Zipcode](./images/predictionWithZip.png?raw=true "Prediction Map with Zipcode")

##### Prediction for 10 days

![Prediction Map for 10 days](./images/prediction10Days.png?raw=true "Prediction for 10 days")


### Exploratory Analysis and Diagnostics

SafeRoad provides additional flexibility for end-users to perform exploratory analysis and model tuning and testing through two Jupyter notebooks with a Spark kernel. The notebooks can be accessed through `notebook\NYC_Collision_Analysis_with_Pyspark.ipynb` and `notebook\SafeRoad_Model_Tuning_and_Diagnostics.ipynb`. Below are examples of exploratory analysis done with Pyspark using various data visualization and model tuning techniques.

![Fatalities by Cause](./images/Fatality_by_Cause.png?raw=true "Fatalities by Cause")

##### Total Fatalities by Listed Cause

![Injuries and Fatalities by Victim](./images/Inj+Fatality_by_Victim.png?raw=true "Injuries and Fatalities by Victim")

##### Total Injuries and Fatalities by Victim Type

![Fatalities Density Maps](./images/Fatality_by_Location.png?raw=true "Fatalities by Year and Victim")

##### Fatalities by Year and Victim

![Injury Heatmap](./images/Injury_by_Month_and_Hour.png?raw=true "Injuries by Month and Hour")

##### Injuries by Month and Hour

## Section 6: Conclusion

### Lessons Learned and the Road Ahead

For our original product we focused our data grid on New York City zip codes; however in discussion with a traffic safety subject matter expert we determined that zip codes are not contextual for our intended end-user. We have provided through the user interface map layers representing NYPD precinct, New York City council districts, and New York City public school districts to be toggled on and off the prediction heat map to aid public policy specialists. A next step for SafeRoad is to create new feature and prediction grids that utilize these more contextual zones, and update the SafeRoad user interface to toggle between various predictions based on the various city zone types.

While our Random Forest model shows fairly high overall accuracy, the precision of the model was very low, leading to a very low F1 score. This is not surprising; we found in our exploratory analysis that the leading cause of serious collisions is due to "driver inattention" and human behavior is an extremely difficult factor to predict. So, perhaps predicting human behavior in one particular geographic area and time is not the best goal. We currently display an overall daily average probability of serious collision by zip code, however in the future SafeRoad may benefit from a model that shows a relative probability of collision compared against all other locations in New York City based on historical data. Additionally, an algorithm designed to find clusters, or "hot-spots" may prove to be more actionable for our intended end-user.

All-in-all, our goal of this project was to show that data could be used to identify the chance of serious collision in order to provide actionable insights to reduce traffic injuries and fatalities. We have made great strides towards that goal with our tool, SafeRoad.
