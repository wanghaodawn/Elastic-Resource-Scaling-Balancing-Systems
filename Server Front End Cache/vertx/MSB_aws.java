/*
 *
 * Cloud Computing Project 2.3 Cache
 *
 * Hao Wang - haow2
 *
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.platform.Verticle;

import java.util.*;
import java.util.LinkedHashMap;


public class MSB extends Verticle {
		
	private String[] databaseInstances = new String[2];

	// Global Variables for LRU_Cache
	private LinkedHashMap<Integer, String> map;
	private final int CACHE_SIZE = 1000;
	private final int NEAR_SIZE_BACK = 40;
	private final int NEAR_SIZE_BEFORE = 10;
	private int map_size = 0;

	/* 
	 * init -initializes the variables which store the 
	 *	     DNS of your database instances
	 */
	private void init() {
		/* Add the DNS of your database instances here */
		databaseInstances[0] = "ec2-52-90-253-61.compute-1.amazonaws.com";
		databaseInstances[1] = "ec2-52-90-115-196.compute-1.amazonaws.com";

		// Init the LinkedHashmap
		map = new LinkedHashMap(CACHE_SIZE, 1.1f, true) {
			// Whether need to remove the eldest entry or not
			protected boolean removeEldestEntry(Map.Entry eldest) {
				return size() > CACHE_SIZE;
			}
		};
	}
	
	/*
	 * checkBackend - verifies that the DCI are running before starting this server
	 */	
	private boolean checkBackend() {
    	try{
    		if(sendRequest(generateURL(0,"1")) == null ||
            	sendRequest(generateURL(1,"1")) == null)
        		return true;
    	} catch (Exception ex) {
    		System.out.println("Exception is " + ex);
    	}

    	return false;
	}

	/*
	 * sendRequest
	 * Input: URL
	 * Action: Send a HTTP GET request for that URL and get the response
	 * Returns: The response
	 */
	private String sendRequest(String requestUrl) throws Exception {
		 
		URL url = new URL(requestUrl);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();

		connection.setRequestMethod("GET");
		connection.setRequestProperty("User-Agent", "Mozilla/5.0");
 
		BufferedReader in = new BufferedReader(
					new InputStreamReader(connection.getInputStream(), "UTF-8"));
		
		String responseCode = Integer.toString(connection.getResponseCode());
		if(responseCode.startsWith("2")){
			String inputLine;
			StringBuffer response = new StringBuffer();
 
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			return response.toString();
    	} else {
    		System.out.println("Unable to connect to "+requestUrl+
    		". Please check whether the instance is up and also the security group settings"); 
    		return null;
    	}   
	}
	/*
	 * generateURL
	 * Input: Instance ID of the Data Center id
	 * Returns: URL which can be used to retrieve the user's details
	 * 			from the data center instance
	 * Additional info: the user's details are cached on backend instance
	 */
	private String generateURL(Integer instanceID, String key) {
		return "http://" + databaseInstances[instanceID] + "/target?targetID=" + key;
	}
	
	/*
	 * generateRangeURL
	 * Input: 	Instance ID of the Data Center
	 * 		  	startRange - starting range (id)
	 *			endRange - ending range (id)
	 * Returns: URL which can be used to retrieve the details of all
	 * 			user in the range from the data center instance
	 * Additional info: the details of the last 1000 user are cached
	 * 					in the database instance
	 * 				
	 */
	private String generateRangeURL(Integer instanceID, String startRange, String endRange) {
		return "http://" + databaseInstances[instanceID] + "/range?start_range="
				+ (startRange) + "&end_range=" + (endRange);
	}

	/* 
	 * retrieveDetails - you have to modify this function to achieve a higher RPS value
	 * Input: the targetID
	 * Returns: The result from querying the database instance
	 */
	private String retrieveDetails(String targetID) {
		try{
			int target_id = Integer.parseInt(targetID);
			if (map.containsKey(target_id)) {
				// Has this targetID in cache, so needn't to send response, but return the string
				String value = map.get(target_id);
				// System.out.println("TargetID: " + targetID + " Hit!");
				return value;
			} else {
				// Doesn't have this targetID in cache, send a new request

				// Get the string of the targetID
//				String value = sendRequest(generateURL(0, targetID));
//				
//				checkMapSize(map, 1);
//				map_size++;
//				map.put(target_id, value);

				// Save all numbers within the range
				int end = target_id + NEAR_SIZE_BACK;
				int begin = Math.max(1, target_id - NEAR_SIZE_BEFORE);
				
				System.out.println("TargetID: " + targetID + " Miss!");
				System.out.println("Add targets in the range from: " + begin + " to: "  + end + " in cache");
				
				String[] ss = retrieveDetails(""+begin, ""+end).split(";");
				
				checkMapSize(map, ss.length);
				map_size += ss.length;
				for (int i = begin; i < end; i++) {
					map.put(i, ss[i-begin]);
				}
				
				String value = ss[NEAR_SIZE_BEFORE];
				
				return value;
			}
		} catch (Exception ex){
			System.out.println(ex);
			return null;
		}
	}
	
	/*
	 * Check the size of map
	 */
	private void checkMapSize(LinkedHashMap map, int val) {
		if (map_size + val >= CACHE_SIZE) {
			// For better performance, create a new cache
			System.out.println("Created a new map!");
			map_size = 0;
			map = new LinkedHashMap(CACHE_SIZE, 1.1f, true) {
				// Whether need to remove the eldest entry or not
				protected boolean removeEldestEntry(Map.Entry eldest) {
					return size() > CACHE_SIZE;
				}
			};
		}
	}

	private String retrieveDetails(String start, String end) {
		try{
			return sendRequest(generateRangeURL(0, start, end));
		} catch (Exception ex){
			System.out.println(ex);
			return null;
		}
	}
	
	/* 
	 * processRequest - calls the retrieveDetails function with the id
	 */
	private void processRequest(String id, HttpServerRequest req) {
		String result = retrieveDetails(id);
		if(result != null)
			req.response().end(result);	
		else
			req.response().end("No resopnse received");
	}

	private void processRequest(String start, String end, HttpServerRequest req) {
		String result = retrieveDetails(start, end);
		if(result != null)
			req.response().end(result);	
		else
			req.response().end("No resopnse received");
	}
	
	/*
	 * start - starts the server
	 */
  	public void start() {
  		init();
		if(!checkBackend()){
  			vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
				public void handle(HttpServerRequest req) {
				    String query_type = req.path();		
				    req.response().headers().set("Content-Type", "text/plain");
				    if(query_type.equals("/target")) {
					    String key = req.params().get("targetID");
					    processRequest(key,req);
				    } else if (query_type.equals("/range")) {
				    	String start = req.params().get("start_range");
				    	String end = req.params().get("end_range");
				    	processRequest(start, end, req);
				    }
			    }               
			}).listen(80);
		} else {
			System.out.println("Please make sure that both your DCI are up and running");
		}
	}
}
