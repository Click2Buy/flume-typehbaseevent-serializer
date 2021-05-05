# Flume Type HBase Serializer

## Build
mvn package

## Install
Copy target/flume-typehbaseevent-serializer-1.0.0.jar in Flume' classpath

## Configure
hbaseagent.sinks.prices.serializer = com.marketconnect.flume.serializer.TypeHbaseEventSerializer
hbaseagent.sinks.prices.serializer.colNames = price,unified_price,available,created_at
hbaseagent.sinks.prices.serializer.types = double,double,string,long
hbaseagent.sinks.prices.serializer.incrementColumn = found 

It will force the type to double / string or long