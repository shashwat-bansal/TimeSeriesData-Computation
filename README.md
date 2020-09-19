# TimeSeriesData-Computation
Data schema:  
Location Index  
Device ID  
Latitude    
Longitude    
Timestamp      
Pin Code  
City  


This is time series data of user locations. Each observation captures location of the
user at the given timestamp.  
Assumptions:  
Person stays at the same place until next observation is received from the user.  

I have designed a system that processes this data and computes approximate average number of devices within
200m radius for every 10 minute time window.

The program takes 30 seconds to run on my local 8-core machine for 1.5 million data points.
