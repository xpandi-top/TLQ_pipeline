package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 * @author dimo
 */
public class ProcessCSV implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        //scanning data line by line and process line by line
        StringWriter sw = new StringWriter();
        HashSet<String> uids= new HashSet<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/M/yy");
        int i = 0;

        Scanner scanner = new Scanner(objectData);

        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            String[] vals = line.split(",");
            if(i==0){
                addHeadertoFile(sw,vals);
            }else {
                addToFile(sw,vals,uids,formatter);
            }
            i++;
        }
        scanner.close();
        //Collect inital data.

        // create file and upload to s3
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/csv");

        // Create new file on S3
        filename = filename.substring(0,filename.length()-4)+"_processed.csv";
        s3Client.putObject(bucketname, filename, is, meta);

        LambdaLogger logger = context.getLogger();
        logger.log("ProcessCSV bucketname:" + bucketname + " filename:" + filename);
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)

        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
        response.setValue("Bucket:" + bucketname + " filename:" + filename+" processed.");
        
        inspector.consumeResponse(response);
        
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    public static void addHeadertoFile(StringWriter sw, String[] header){
        int n = header.length;
        for (String val: header){
            sw.append(val);
            sw.append(",");
        }
        sw.append("Order Processing Time");
        sw.append(",");
        sw.append("Gross Margin");
        sw.append("\n");

    }

    public static void addToFile(StringWriter sw, String[] values, HashSet<String> uids, DateTimeFormatter formatter){
        if (values.length<14) return;
        if(uids.contains(values[6])) return;
        uids.add(values[6]);
        int n = values.length;
        for (int i = 0;i<n;i++){
            String val = values[i];
            if(i==4){ // order priority column
                switch (val) {
                    case "C":
                        val = "Critical";
                        break;
                    case "L":
                        val = "Mow";
                        break;
                    case "M":
                        val = "Medium";
                        break;
                    case "H":
                        val = "High";
                        break;
                }
            }
            sw.append(val);
            sw.append(",");
        }
        // append days betwen ship date and order date
        LocalDate shiped = LocalDate.parse("30/11/21",formatter);
        LocalDate ordered = LocalDate.parse("10/11/21",formatter);
        int days = (int) ChronoUnit.DAYS.between(ordered,shiped);
        sw.append(Integer.toString(days));
        sw.append(",");
        // append Grass Margin total profit/revenue
        float totalProfit = Float.parseFloat(values[13]);
        float totalRevenue = Float.parseFloat(values[11]);
        float margin = totalProfit/totalRevenue;
        sw.append(Float.toString(margin));
        sw.append("\n");
    }

    public static void main(String[] args) throws IOException {
        String filename = "/Users/dimo/Downloads/100SalesRecords.csv";
        File file = new File(filename);
        StringWriter sw = new StringWriter();
        HashSet<String> uids= new HashSet<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/M/yy");
        int i = 0;
        try{
            Scanner inputStream = new Scanner(file);
            while (inputStream.hasNext()){
                String line = inputStream.nextLine();
                String[] vals = line.split(",");
                if(i==0){
                    addHeadertoFile(sw,vals);
                }else {
                    addToFile(sw,vals,uids,formatter);
                }
                i++;
            }
            // after loop close;
            inputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        FileWriter fw = new FileWriter("100SalesRecordsProcessed.csv");
        fw.write(sw.toString());
        fw.close();
    }
}
