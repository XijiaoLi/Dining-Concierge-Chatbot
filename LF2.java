package example;
//Lambda
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
//SNS
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
//SQS
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
//JAVA
import java.util.*;
import java.util.Map.Entry;
//POST
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
//JSON
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONArray;
//DynamoDB

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;


public class Hello{
  
  public String handleRequest(ScheduledEvent input, Context context) {
    //read from SQS
    AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    String queueUrl = "https://sqs.us-east-1.amazonaws.com/585623052567/cloud_sqs.fifo";

    ReceiveMessageRequest sqsrequest =  new ReceiveMessageRequest(queueUrl);
                                              
    List<Message> messages = sqs.receiveMessage(sqsrequest).getMessages();
    
    if(messages.size() == 0){
      return String.format("No Request");
    }

    Message message = messages.get(0);

    System.out.println("  MessageId:     " + message.getMessageId());
    System.out.println("  ReceiptHandle: " + message.getReceiptHandle());
    System.out.println("  MD5OfBody:     " + message.getMD5OfBody());
    System.out.println("  Body:          " + message.getBody());

    sqs.deleteMessage(queueUrl,message.getReceiptHandle());

    //Send request to ES
    JSONObject mesobj = JSON.parseObject(message.getBody());
    String cuisine = mesobj.getJSONObject("Cuisine").getString("StringValue");
    JSONObject object = JSON.parseObject("{\"size\":20,\"query\":{\"match\":{ \"cuisine\":\""+cuisine+"\"}}}");
    String ESresponse;
    

    List<String> busIDs = new ArrayList();
    int count = 0;
    try {
      ESresponse = sendPost("https://search-restaurants-45jfver7oajopu65sxkjdb37ma.us-east-1.es.amazonaws.com/restaurants/_search",JSON.toJSONString(object));
      object = JSON.parseObject(ESresponse);
      object = object.getJSONObject("hits");
      JSONArray hits = object.getJSONArray("hits");

      for (Iterator iterator = hits.iterator(); iterator.hasNext();) {
        JSONObject job = (JSONObject) iterator.next();
        job = job.getJSONObject("_source");
        count ++;
        busIDs.add(job.get("businessID").toString());
      }
    }catch (Exception e) {
      System.out.println("发送 POST 请求出现异常！"+e);
      e.printStackTrace();
    }

    //Send request to DB
    String tableName = "yelp-restaurants";
    String dbres;
    JSONObject dbresobj;

    String[] resString={"","",""};

    for(int k=0; k<count && k<3; k++){
      dbres = query(tableName,busIDs.get(k));
      dbresobj = JSON.parseObject(dbres);
      resString[k] = String.valueOf(k+1)+". "+dbresobj.getString("name")+", located at "+dbresobj.getString("location")+" ";
    }

    //send by SNS
    
    AmazonSNSClient snsClient = new AmazonSNSClient();
    String phoneNumber = mesobj.getJSONObject("PhoneNumber").getString("StringValue");
    Map<String, MessageAttributeValue> smsAttributes = new HashMap<String, MessageAttributeValue>();
    String content = "Hello! Here are my "+cuisine+" restaurant suggestions for "+mesobj.getJSONObject("NumberOfPeople").getString("StringValue")+" people, for "+mesobj.getJSONObject("Date").getString("StringValue")+" at "+mesobj.getJSONObject("DiningTime").getString("StringValue")+": ";
    for(int k=0; k<count && k<3; k++){
      content += resString[k];
    }
    content += ". Enjoy your meal!";

    System.out.println("Send Text To:"+phoneNumber+" with :"+content);

    smsAttributes.put("AWS.SNS.SMS.SenderID", new MessageAttributeValue()
        .withStringValue("KOKOtest") //The sender ID shown on the device.
        .withDataType("String"));
    smsAttributes.put("AWS.SNS.SMS.SMSType", new MessageAttributeValue()
        .withStringValue("Promotional") //Sets the type to promotional.
        .withDataType("String"));

    
    PublishResult result = snsClient.publish(new PublishRequest()
                        .withMessage(content)
                        .withPhoneNumber("+1"+phoneNumber)
                        .withMessageAttributes(smsAttributes));

    return String.format("Hello KOKO");
  }

  private String query(String tableName, String key) {
    AmazonDynamoDBClient client = new AmazonDynamoDBClient();
    DynamoDB dynamoDB = new DynamoDB(client);
    Table table = dynamoDB.getTable(tableName);
    String Qid = key;
    QuerySpec spec = new QuerySpec()
        .withKeyConditionExpression("businessID = :v_id")
        .withValueMap(new ValueMap()
          .withString(":v_id", Qid));
    
    ItemCollection<QueryOutcome> items = table.query(spec);

    Iterator<Item> iterator = items.iterator();
    Item item = null;
    while (iterator.hasNext()) {
        item = iterator.next();
        System.out.println(item.toJSON());
    }
    return item.toJSONPretty();
  }

  private String sendPost(String url, String Request) throws Exception {
    System.out.println("\nSending 'POST' request to URL : " + url);
    System.out.println("Request Data : " + Request);

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		//add reuqest header
		con.setRequestMethod("POST");
		con.setRequestProperty("Content-Type", "application/json");
		
    //Deal data
    byte[] databytes = Request.toString().getBytes("UTF-8");

		// Send post request
		con.setDoOutput(true);
		con.getOutputStream().write(databytes);

		int responseCode = con.getResponseCode();

		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		//print result
		return response.toString();

	}
}