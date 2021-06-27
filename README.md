# Big-Data-Project-Youtube-analysis-

# YouTube-Analysis

This project analyzes YouTube records. Kaggle provided a set of statistics (including views, likes, category, and comments) for videos trending on YouTube.  An overview of the data is located here:
https://www.kaggle.com/datasnaek/youtube.  


## Objective

Calculate top statistics for the YouTube data set from Kaggle using a single Hadoop MapReduce program. We will also analyze how many of total records were there and how many among them was good or bad record.


### Mapper

Mapper program will extract good records out of all and discard the bad records. It will also calculate total number of good and bad records.

### Reducer

Reducer program will find the most viewed, liked and disliked videos of all and send it to driver with a key.
