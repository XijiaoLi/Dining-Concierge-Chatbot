package com.amazonaws.samples;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class CreateTableAndLoadData {
	
	static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    static DynamoDB dynamoDB = new DynamoDB(client);
	
	public static void createTable(String tableName) {

        try {
            System.out.println("Creating table...");
            Table table = dynamoDB.createTable(tableName,
                Arrays.asList(new KeySchemaElement("id", KeyType.HASH)),
                Arrays.asList(new AttributeDefinition("id", ScalarAttributeType.S)), new ProvisionedThroughput(5L, 5L));
            
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());
        }
        catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
	}
	
	public static String readJson(String fileName) {
		String jsonStr = "";
		try {
			File jsonFile = new File(fileName);
			FileReader fileReader = new FileReader(jsonFile);

            Reader reader = new InputStreamReader(new FileInputStream(jsonFile),"utf-8");
            int ch = 0;
            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static void parseData(String tableName, String jsonStr) {
		Table table = dynamoDB.getTable(tableName);
		JSONObject jsonData = new JSONObject(jsonStr);
		JSONArray jsonArray = jsonData.getJSONArray("data");
		
		for (int i = 0; i < jsonArray.length(); i++) {
			
			JSONObject business = jsonArray.getJSONObject(i);
			String id = business.getString("businessID");
			String name = business.getString("name");
			JSONArray cuisine = business.getJSONArray("cuisine");
			Set<String> set = new HashSet<>();
			for (int j = 0; j < cuisine.length(); j++) {
				set.add(cuisine.getString(j));
			}
			String location = business.getString("location");
			String zip_code = business.getString("zip_code");
			String phone = business.getString("phone");
			JSONObject coordinates = business.getJSONObject("coordinates");
			Map<String, Float> map = new HashMap<>();
			map.put("latitude", coordinates.getFloat("latitude"));
			map.put("longitude", coordinates.getFloat("longitude"));
			
			int review_count = business.getInt("review_count");
			double rating = business.getDouble("rating");
			
			
			System.out.print("Putting item " + i + "...");
			Item item = new Item().withPrimaryKey("businessID", id);
			if (!name.isEmpty()) {
				item.withString("name", name);
			}
			if (!set.isEmpty()) {
				item.withStringSet("cuisine", set);
			}
			if (!location.isEmpty()) {
				item.with("location", location);
			}
			if (!zip_code.isEmpty()) {
				item.withString("zip_code", zip_code);
			}
			if (!phone.isEmpty()) {
				item.withString("phone", phone);
			}
			if (!map.isEmpty()) {
				item.withMap("coordinates", map);
			}
			item.withNumber("review_count", review_count).withNumber("rating", rating).withString("insertedAtTimestamp", dateFormatter.format(new Date()));
			
			try {
				table.putItem(item);
				System.out.println("Done!");
			} catch (Exception e) {
				System.out.println("Wrong: id_" + id);
				e.printStackTrace();
			}	
		}
		System.out.println("Done!");
	}
	
	public static ArrayList<String> query(String tableName) {
		ArrayList<String> result = new ArrayList<>();
		Table table = dynamoDB.getTable(tableName);
		String Qid = "-_fDmhClugYdfy19qcNQpw";
		QuerySpec spec = new QuerySpec()
			    .withKeyConditionExpression("businessID = :v_id")
			    .withValueMap(new ValueMap()
			    		.withString(":v_id", Qid));
		
		ItemCollection<QueryOutcome> items = table.query(spec);

		Iterator<Item> iterator = items.iterator();
		Item item = null;
		while (iterator.hasNext()) {
		    item = iterator.next();
		    result.add(item.toJSONPretty());
		}
		return result;
	}
	
	public static void main(String[] args) {
		String tableName = "yelp-restaurants";
		String fileName = "yelp-restaurants.json";
		
		createTable(tableName);
		String jsonStr = readJson(fileName);
		parseData(tableName, jsonStr);
		ArrayList<String> result = query(tableName);
		System.out.println(result);
	}
	
}
