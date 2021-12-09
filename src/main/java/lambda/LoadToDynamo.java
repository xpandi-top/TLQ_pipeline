package lambda;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import saaf.Response;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class LoadToDynamo implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        //String bucketname = request.getBucketname();
        String filename = request.getFilename();
        Response r = new Response();
        //loadTotoAurora(filename);
        LambdaLogger logger = context.getLogger();

        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        inspector.consumeResponse(r);
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    public static void loadToDynamo(String filename) {
        String bucketname = "test.bucket.sales.dimo";
        //filename = "100SalesRecords_processed.csv";//
        // connect to s3
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        //scanning data line by line and process line by line
        StringWriter sw = new StringWriter();
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/M/yy");
        Scanner scanner = new Scanner(objectData);
        scanner.nextLine();
        String headers = "Region,Country,Item_Type,Sales_Channel,Order_Priority,Order_Date,Order_ID,Ship_Date,Units_Sold,Unit_Price,Unit_Cost,Total_Revenue,Total_Cost,Total_Profit,Order_Processing_Time,Gross_Margin";
        String[] headerList = headers.split(",");
        //Properties properties = new Properties();

        // connect to dynamodb
        BasicAWSCredentials AWS_CREDENTIALS = new BasicAWSCredentials(
                "MMM",
                "MMM"
        );
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(AWS_CREDENTIALS))
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);
        String tableName = filename.substring(0,filename.length()-4);

        Table table;
        try {
            System.out.println("Attempting to create table; please wait...");
            CreateTableRequest createTableRequest = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(List.of(new KeySchemaElement("Order_ID", KeyType.HASH)))
                    .withAttributeDefinitions(List.of(new AttributeDefinition("Order_ID", ScalarAttributeType.S)))
                    .withBillingMode("PAY_PER_REQUEST");

            table = dynamoDB.createTable(createTableRequest);
            table.waitForActive();
            System.out.print("Success.  Table status: " + table.getDescription().getTableStatus());
            table =dynamoDB.getTable(tableName);

        }
        catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
            table=dynamoDB.getTable(tableName);
        }
        int count=0;
        while (scanner.hasNext()){
            count++;
            String currLine = scanner.nextLine();
            String[] vals = currLine.split(",");
            Map<String,Object> test = new HashMap<>();
            for (int i =0; i< headerList.length;i++){
                test.put(headerList[i],vals[i]);
            }
            try {
                table.putItem(Item.fromMap(test));
                if(count%1000==1) System.out.print("*");
            } catch (Exception e) {
                System.err.println("Unable to put line of NO."+count);
                System.err.println(e.getMessage());
                return;
            }
        }
        System.out.println("all finished: "+count);
    }

    public static void main(String[] args) {
        String[] filenames = new String[]{
        "1000000SalesRecords_processed.csv",
        "100000SalesRecords_processed.csv",
        "10000SalesRecords_processed.csv",//done
        "1000SalesRecords_processed.csv",
        "100SalesRecords_processed.csv",//done
        "1500000SalesRecords_processed.csv",
        "500000SalesRecords_processed.csv",
        "50000SalesRecords_processed.csv",
        "5000SalesRecords_processed.csv"
        };
        for(String filename:filenames){
            System.out.println("processing "+filename);
            loadToDynamo(filename);
        }

    }


}
