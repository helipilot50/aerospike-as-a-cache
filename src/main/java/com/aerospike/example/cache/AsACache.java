package com.aerospike.example.cache;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;


/**
@author Peter Milne
*/
public class AsACache {
	private AerospikeClient client;
	private String seedHost;
	private int port;
	private String namespace;
	private String set;
	private WritePolicy writePolicy;
	private Policy policy;

	private static Logger log = Logger.getLogger(AsACache.class);
	public AsACache(String host, int port, String namespace, String set) throws AerospikeException {
		this.client = new AerospikeClient(host, port);
		this.seedHost = host;
		this.port = port;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	public static void main(String[] args) throws AerospikeException {
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: 127.0.0.1)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set (default: demo)");
			options.addOption("u", "usage", false, "Print usage.");

			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);


			String host = cl.getOptionValue("h", "127.0.0.1");
			String portString = cl.getOptionValue("p", "3000");
			int port = Integer.parseInt(portString);
			String namespace = cl.getOptionValue("n", "test");
			String set = cl.getOptionValue("s", "demo");
			log.debug("Host: " + host);
			log.debug("Port: " + port);
			log.debug("Namespace: " + namespace);
			log.debug("Set: " + set);

			@SuppressWarnings("unchecked")
			List<String> cmds = cl.getArgList();
			if (cmds.size() == 0 && cl.hasOption("u")) {
				logUsage(options);
				return;
			}

			AsACache as = new AsACache(host, port, namespace, set);

			as.work();

		} catch (Exception e) {
			log.error("Critical error", e);
		}
	}
	/**
	 * Write usage to console.
	 */
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = AsACache.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		log.info(sw.toString());
	}

	public void work() throws Exception {
		Flights flights = new Flights<String, JSONObject>(this.client, this.namespace, this.set, 300); // Time to live of 5 mins
		log.info(flights.get("DFW"));
		log.info(flights.get("SFO"));
		log.info(flights.get("BWI"));
		log.info(flights.get("SJC"));
	}

}
