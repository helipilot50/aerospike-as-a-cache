/* 
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.example.cache;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.WritePolicy;

public class Flights<K, V> implements Map<K, V> {
	private static Logger log = Logger.getLogger(Flights.class);
	public static final String FLT_DATA_BIN = "flt_data_bin";


	
	private AerospikeClient asClient;
	private String namespace;
	private String set;
	private WritePolicy writePolicy;
	private int defaultTTL;
	private JSONParser parser;

	public Flights(AerospikeClient client, String namespace, String set, int ttl){
		this.asClient = client;
		this.namespace = namespace;
		this.set = set;
		this.defaultTTL = ttl;
		this.writePolicy = new WritePolicy();
		this.writePolicy.expiration = this.defaultTTL;
		this.parser = new JSONParser();
	}

	private Key getKey(Object key) throws AerospikeException{
		return new Key(this.namespace, this.set, Value.get(key));
	}
	private void handleAerospikeError(AerospikeException e){
		log.error("Aerospike error", e);
	}
	private Map<String, String> parseMap(final String input, String seperator) {
        final Map<String, String> map = new HashMap<String, String>();
        for (String pair : input.split(seperator)) {
            String[] kv = pair.split("=");
            map.put(kv[0], kv[1]);
        }
        return map;
    }
	@Override
	public void clear() {
		try {
			Node[] nodes = this.asClient.getNodes();
			for (Node node : nodes){
				Info.request(node, "set-config:context=namespace;id=" 
						+ this.namespace 
						+ ";set=" + this.set 
						+ ";set-delete=true;");
			}
		} catch (AerospikeException e) {
			handleAerospikeError(e);
		}
	}

	@Override
	public boolean containsKey(Object key) {
		try {
			return this.asClient.exists(null, getKey(key));
		} catch (AerospikeException e) {
			log.error("Aerospike error", e);
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		return false;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return null;
	}

	@Override
	public V get(Object key) {
		try {
			Record record = this.asClient.get(null, getKey(key), FLT_DATA_BIN);
			if (record == null){
				// fetch from source
				log.debug(key + " not in cache, fetching from source...");
				JSONObject result = getFromSource((String)key);
				// Save in Aerospike
				this.asClient.put(this.writePolicy, getKey(key), new Bin(FLT_DATA_BIN, Value.getAsMap(result)));
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

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public Set<K> keySet() {
		try {
			final Collection<byte[]> keySet = new ArrayList<byte[]>();
			this.asClient.scanAll(null, this.namespace, this.set, new ScanCallback() {

				@Override
				public void scanCallback(Key key, Record record) throws AerospikeException {
					keySet.add(key.digest);
				}
			});
			return (Set<K>) keySet;
		} catch (AerospikeException e) {
			handleAerospikeError(e);
		}
		return null;
	}

	@Override
	public V put(K key, V value) {
		// Not implemented -- read only cache
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> arg0) {
		// Not implemented -- read only cache

	}

	@Override
	public V remove(Object key) {
		try {
			this.asClient.delete(this.writePolicy, getKey(key));
		} catch (AerospikeException e) {
			handleAerospikeError(e);
		}
		return null;
	}

	@Override
	public int size() {
		try {
			int objectCount = 0;
			Node[] nodes = this.asClient.getNodes();
			for (Node node : nodes){
				String result = Info.request(node, "sets/test/users");
				objectCount += Integer.parseInt(parseMap(result, ":").get("n_objects"));
			}
			return objectCount;
		} catch (AerospikeException e) {
			handleAerospikeError(e);
		}
		return 0;
	}

	@Override
	public Collection<V> values() {

		try {
			final Collection<V> values = new ArrayList<V>();
			this.asClient.scanAll(null, this.namespace, this.set, new ScanCallback() {

				@Override
				public void scanCallback(Key key, Record record) throws AerospikeException {
					JSONObject data = new JSONObject((Map) record.getValue(FLT_DATA_BIN));
					values.add((V) data);
				}
			}, FLT_DATA_BIN);
			return values;
		} catch (AerospikeException e) {
			handleAerospikeError(e);
		}
		return null;
	}

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


}
