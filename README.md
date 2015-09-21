# Aerospike as a cache

##Problem
You need to provide a service with up-to-date data obtained from external data sources. 

The information in you service is time sensitive. Individual data elements (tuples) becomes stale after a period of time and need to be refreshed or updated. 

When a tuple is refreshed, you pay the data provider, so you only want to obtain the data once during a period of validity.

##Solution
The solution is a read-only cache that is lazy loaded.
 
When a request is made for a tuple, the cache is checked, if found the tuple is returned, if not it is obtained from the source, cached for later use, and returned to the caller.

When a tuple becomes stale, after a time period, it is removed from the cache. A subsequent request for that tuple will result in a refresh from the source.

Aerospike is similar, in concept, to a Distributed Hash Table (DHT). In other words, a really big hashtable. Aerospike also has an in-build time-to-live feature on the record (tuple) level.

### Build
The source code for this solution is available on GitHub at https://github.com/aerospike/aerospike-as-a-cache.git. 

Clone the GitHub repository to your local file system with:
```bash
git clone https://github.com/aerospike/aerospike-as-a-cache.git
```

Maven is used to build this example. From the rood directory of the project, issue the following command:
```bash
mvn clean package
```
A JAR files will be produced in the subdirectory 'target': aerospike-as-a-cache-\<version\>-full.jar. This is a runnable jar complete with all the dependencies packaged

### Run

To run the example, you will specify the address of the cluster, and the set to delete. The following command connects to a cluster located at the server ‘192.168.1.15’, a namespace of 'test and a set named ‘demo’.
```bash
java -jar aerospike-as-a-cache-1.0.0-full.jar -h 192.168.1.15 -p 3000 -n test -s demo
```


##Discussion

This example is a simple Java application that requests information on Airports in the United States. It sends a RESTful http request for a specific airport, and receives the results in JSON format.

The source data comes from the US Federal Aviation Authority's RESTful service at [FAA Web Services](http://services.faa.gov/).

### AsACache class
The main class com.aerospike.examples.cache.AsACache is a simple shell that is a elementary console application. It connects to a Cluster using the the arguments passed in from the command line. 

The interesting code is done in the `work()` method:
```java
	public void work() throws Exception {
		Flights flights = new Flights<String, JSONObject>(this.client,
					this.namespace, 
					this.set, 
					300); // Time to live of 5 mins
		log.info(flights.get("DFW"));
		log.info(flights.get("SFO"));
		log.info(flights.get("BWI"));
		log.info(flights.get("SJC"));
	}
```
Here you see the creation of an object of type Flights, it is passed as AerospikeClient instance, an namespace name, a set name and a time to live value (in seconds). 

Once instantiated, the `get()` method is called to obtain details of 4 airports: DFW, SFO, BWI and SJC. This information, in JSON, is printed to the console.

The [IATA airport code](http://www.iata.org/publications/Pages/code-search.aspx) is the key to the data.

### Flights class

The com.aerospike.examples.cache.Flights class implements the Java [Map](http://docs.oracle.com/javase/7/docs/api/java/util/Map.html) interface, and it exposes the Aerospike database as a really big Hashtable. 

The application developer simply uses a familiar interface and is abstracted from the implementation details.

Look at the implementation of the `get()` method:
```java
	public V get(Object key) {
		try {
			Record record = this.asClient.get(null, getKey(key), FLT_DATA_BIN);
			if (record == null){
				// fetch from source
				log.debug(key + " not in cache, fetching from source...");
				JSONObject result = getFromSource((String)key);
				// Save in Aerospike
				this.asClient.put(this.writePolicy, getKey(key), 
						new Bin(FLT_DATA_BIN, Value.getAsMap(result)));
				return (V) result;
			} else {
				log.debug(key + " cached");
				return (V) new JSONObject((Map) record.getValue(FLT_DATA_BIN));
			}
		} catch (AerospikeException e) {
			handleAerospikeError(e);
		} catch (Exception e) {
			log.error(e);
		}
		return null;
	}

```
First we attempted to get a JSON object from Aerospike using the airport code as the primary key. If we have already looked for this airport in the last 5 minutes (300 seconds), it will be cached in Aerospike.

If it is not found, we co to the source and fetch it with the `getFromSource()' method. 
```java
JSONObject result = getFromSource((String)key);
```
Then we store it in Aerospike. 

So how does the data become stale? Note the [write policy](http://www.aerospike.com/docs/client/java/usage/kvs/write.html) in the Aerospike `put()`. 
```java
this.asClient.put(this.writePolicy, getKey(key), 
						new Bin(FLT_DATA_BIN, Value.getAsMap(result)));
```
We initialized this write policy object with the expiration value of 300 seconds. This means that the record will automatically become unavailable 300 seconds after it was written, an be physically removed from the database, at a future time, buy the automatic background defrag task.

So we have configured the "cache" to automatically expire stale data. No complex cleanup query, no cron job, no headache. 

lets look at how we get the data from the source. Remember that Aerospike delivers results in single digit millisecond latencies. The source data could take 1000x longer.

Consider the `getFromSource()` method:
```java
	private JSONObject getFromSource(String airport) throws Exception {
		// http://services.faa.gov/airport/status/IAD?format=son
		String url = String.format("http://services.faa.gov/airport/status/%s?format=JSON", airport);
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		//add request header
		con.setRequestMethod("GET");
		con.setRequestProperty("Accept", "application/JSON");
 
		int responseCode = con.getResponseCode();
		log.debug("\nSending 'GET' request to URL : " + url);
		log.error("Response Code : " + responseCode);
 
		// return result as JSONobject
		JSONObject result =  (JSONObject) parser.parse(new InputStreamReader(con.getInputStream()));
		return result;
	}
```
We form a simple GET request from the FAA RESTful service. It is returned as a JSON string, and we parse it into a JSONObject See [JSON simple](https://code.google.com/p/json-simple/).



