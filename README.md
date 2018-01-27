@author rohit-kulkarni

# Presto-MapD-Connector

Presto-MapD-Connector makes it possible to query [MapD](https://www.mapd.com/) via [Presto](https://prestodb.io/).

# Installation

## Requirements

* [MapD](https://www.mapd.com/platform/downloads/)
* [Presto v0.170](https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.170/presto-server-0.170.tar.gz)

## Setup

1. Clone this repository and cd into it
```
git clone git@github.com:NVIDIA/presto-mapd-connector.git
cd presto-mapd-connector
```

2. Create a `lib` directory and copy the mapd jdbc jar into it (`mapdjdbc-1.0-SNAPSHOT-jar-with-dependencies.jar`)
```
mkdir lib
cp $MAPD_HOME/mapdjdbc-1.0-SNAPSHOT-jar-with-dependencies.jar lib/
```

3. Build/package the project
```
mvn package
```

4. Copy the jar to your presto plugin directory
```
mkdir $PRESTO_HOME/plugin/mapdjdbc
cp presto-mapd-jdbc-1.0-jar-with-dependencies.jar $PRESTO_HOME/plugin/mapdjdbc
```

5. Make a mapdjdbc.properties file
```
mkdir -p $PRESTO_HOME/etc/catalog && touch $PRESTO_HOME/etc/catalog/mapdjdbc.properties
```

6. In the properties file, enter your credentials to connect to mapd...
```
connector.name=mapdjdbc
connection-url=jdbc:mapd:localhost:9091:mapd
connection-user=<username>
connection-password=<password>
```

7. You can now start mapd and presto with the ability to query mapd from presto!
