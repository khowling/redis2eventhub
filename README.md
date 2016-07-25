


Send Redis List into Event Hub, pops messages from the queue (brpop), and sneds to Azure event hub using the AMQP 1.0 protocol.  


Files

index.js :  sends data from a Redis queue to an Azure eventhub
rx.js : receive data from the hub (just for debugging verification)
test-bench.js: pumps data into redis using a cluster for 4 processes (bench testing)

environment variables
REDIS_URL:  <default: 'redis://localhost:6379/3'>
REDIS_CHANNEL: The Redis List Key <default: 'clickpath'>
*ENTITY_PATH: The Azure eventhub Entity name
*AMQP_URL: The Azure Servicebus AMQP Url <example: 'amqps://<AccessKey>:<Access>@<namespace>.servicebus.windows.net'>

* = requried

DEBUG="amqp10:client"  to debug (or $env:DEBUG = "amqp10:*")


Event Hub has Publishers and Consumers.

Event Hubs uses a partitioned consumer pattern in which each consumer only reads a specific subset, or partition, of the message stream. Partitions are a data organization mechanism and are  related to the degree of downstream parallelism required in consuming applications than to Event Hubs throughput. Partitions retain data for a configured retention time that is set at the Event Hub level. This setting applies across all partitions in the Event Hub. Events expire on a time basis; you cannot explicitly delete them.  The choice of the number of partitions in an Event Hub directly related to the number of concurrent readers you expect to have, it is generally best to avoid sending data to specific partitions.

A partition key is a value that is used to map incoming event data into specific partitions for the purposes of data organization.  If you don't specify a partition key when publishing an event, a round robin assignment is used

A consumer group is a view (state, position, or offset) of an entire Event Hub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets.  If you want to write event data to long-term storage, then that storage writer application is a consumer group. Complex event processing is performed by another, separate consumer group. You can only access partitions through a consumer group. There is always a default consumer group in an Event Hub ($Default), and you can create up to 20 consumer groups 
