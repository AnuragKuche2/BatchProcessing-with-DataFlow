# BatchProcessing-with-DataFlow

Hands on Apache Beam, building data pipelines in Python

Apache Beam is an open-source SDK which allows you to build multiple data pipelines from batch or stream based integrations and run it in a direct or distributed way. You can add various transformations in each pipeline. But the real power of Beam comes from the fact that it is not based on a specific compute engine and therefore is platform independant. You declare which 'runner' you want to use to compute your transformation. It is using your local computing resource by default, but you can specify a Spark engine for example or Cloud Dataflow.

In this project, I will create a pipeline ingesting a csv file, Grouping the data based on neighbourhood and then sorting the values based on the popularity. The goal here is not to give an extensive tutorial on Beam, dataFlow as well as GCP, but rather to give you an overall idea of what you can do with it and if it is worth for you going deeper in building custom pipelines with Beam.


# Installation
At the date of this article Apache Beam (2.8.1) is only compatible with Python 2.7, however a Python 3 version should be available soon. If you have python-snappy installed, Beam may crash. This issue is known and will be fixed in Beam 2.9.

```
pip install apache-beam 
```


# Data
This public dataset is part of Airbnb, and the original source can be found on their website. Since 2008, guests and hosts have used Airbnb to expand on traveling possibilities and present more unique, personalized way of experiencing the world. This dataset describes the listing activity and metrics in NYC, NY for 2019.

