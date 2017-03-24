# Cloud_Computing_Spark

Log Analysis using Spark


## Run 
- Local 
spark-submit --master local[*] user.py user_artists.dat 
spark-submit --master local[*] log_analysis.py short.txt urlCount /assets/js/lowpro.js 


- Cluster 
spark-submit --master spark://Abdulazizs-MacBook-Pro.local:7077 user.py user_artists.dat
spark-submit --master spark://Abdulazizs-MacBook-Pro.local:7077 log_analysis.py short.txt urlCount /assets/js/lowpro.js 


## Log Analysis args:
  - urlCount /assets/js/lowpro.js 
  - ipMax
  - urlMax
