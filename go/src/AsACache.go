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
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	. "github.com/aerospike/aerospike-client-go"
	"io/ioutil"
	"log"
	"net/http"
)

/**
@author Peter Milne
*/
var seedHost = flag.String("h", "127.0.0.1", "Server hostname (default: 127.0.0.1)")
var port = flag.Int("p", 3000, "Server port (default: 3000)")
var namespace = flag.String("n", "test", "Namespace (default: test)")
var set = flag.String("s", "demo", "Set (default: demo)")
var usage = flag.String("u", "", "Print usage.")
var writePolicy *WritePolicy
var client *Client

const FLT_DATA_BIN string = "flt_data_bin"

func panicOnError(err error){
    if err != nil {
    	log.Fatal(err)
        panic(err)
    }
}


func main() {
	flag.Parse()
	var err error
	client, err = NewClient(*seedHost, *port)
	if err != nil {
		fmt.Printf("Cannot connecto to Aerospike: %v", err)
		return
	}
	writePolicy = NewWritePolicy(0, 300) // Time to live of 5 mins
	fmt.Printf("DFW: %v\n", GetAirport(client, "DFW"))
	fmt.Printf("SFO: %v\n", GetAirport(client, "SFO"))
	fmt.Printf("BWI: %v\n", GetAirport(client, "BWI"))
	fmt.Printf("SJC: %v\n", GetAirport(client, "SJC"))
}

func GetAirport(client *Client, airport string) map[interface{}]interface{} {
	key, _ := NewKey(*namespace, *set, airport)
	record, _ := client.Get(nil, key, FLT_DATA_BIN)
	if record == nil {
		// fetch from source
		fmt.Printf("%s not in cache, fetching from source...\n", airport)
		result := getFromSource(airport)
		// Save in Aerospike
		//bin := NewBin(FLT_DATA_BIN, result)
		//fmt.Printf("Bin: %v\n", bin)
		record, err := client.Operate(writePolicy, key, PutOp(NewBin(FLT_DATA_BIN, result)), GetOpForBin(FLT_DATA_BIN))
		panicOnError(err)
		return record.Bins[FLT_DATA_BIN].(map[interface{}]interface{})
	} else {
		fmt.Printf("%s cached\n", airport)
		client.Touch(nil, key)
		record, err := client.Get(nil, key, FLT_DATA_BIN)
		panicOnError(err)
		result := record.Bins[FLT_DATA_BIN]
		return result.(map[interface{}]interface{})
	}
}

func getFromSource(airport string) map[string]interface{} {
	var data map[string]interface{}
	// http://services.faa.gov/airport/status/IAD?format=json
	httpClient := &http.Client{}
	url := fmt.Sprintf("http://services.faa.gov/airport/status/%s?format=JSON", airport)
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Add("Accept", "application/JSON")
	resp, err := httpClient.Do(req)
	panicOnError(err)
	result, err := ioutil.ReadAll(resp.Body)
	panicOnError(err)
	defer resp.Body.Close()
	json.Unmarshal(result, &data)
	//fmt.Printf("Result %v\nData %v\n", result, data);
	return data

}
