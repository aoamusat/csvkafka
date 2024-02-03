## About 

This project demonstrates how to use Apache Kafka Consumer/Producer API in python

The producer function produces events by sending rows from a huge csv file. The csv file reader leverages Python generators to yield each row for the producer. Each row is serialized using the AvroSerializer before sending the serialized events to the topic.