/*
 * Hao Wang - haow2
 */

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import java.net.HttpURLConnection;
import java.net.URL;

public class azure_horizontal_scaling {

    // Record the number of dc and lg
    private static int num_dc = 0;
    private static int num_lg = 0;

    // The ratio to control the name to prevent identical name
    private static int step = 1;

    // Stores the information
    private static String ami_lg = "ami-8ac4e9e0";
    private static String ami_dc = "ami-8ac4e9e0";
    private static String instance_lg = "m3.medium";
    private static String instance_dc = "m3.medium";
    private static String tag_key = "Project";
    private static String tag_value = "2.1";
    private static String andrew_id = "";
    private static String submission_id = "";


    /*
     * Setup Load Balancer
     */
    private static void basicSetup(String dns_lg) throws Exception {

        String url_lg = "http://" + dns_lg + "/password?passwd=" + submission_id + "&andrewid=" + andrew_id;

        URL url = new URL(url_lg);

        int response_code;
        HttpURLConnection url_conn;

        // Send user information to Load Generator
        do {
            Thread.sleep(70000);
            System.out.println("Sending HTTPRequest to URL of LG: " + url_lg);
            url_conn = (HttpURLConnection) url.openConnection();
            response_code = url_conn.getResponseCode();
            System.out.println("The response code is: " + response_code);

        } while (response_code != 200);

        System.out.println("\nSuccessfully sent andrewid and submission id!\n");

    }

    /*
     * Send the info of Data Center to Load Balancer
     */
    private static void setupDataCenter(String dns_dc, String dns_lg, boolean isFirst) throws Exception {

        String url_dc;
        if (isFirst) {
            url_dc = "http://" + dns_lg + "/test/horizontal?dns=" + dns_dc;
        } else {
            url_dc = "http://" + dns_lg + "/test/horizontal/add?dns=" + dns_dc;
        }

        int response_code = 0;
        HttpURLConnection url_conn;
        URL url = new URL(url_dc);

        // Send the information of Data Center to Load Generator
        do {
            Thread.sleep(10000);
            System.out.println("Sending HTTPRequest to URL of LG the info of DC: " + url_dc);
            url_conn = (HttpURLConnection) url.openConnection();
            response_code = url_conn.getResponseCode();
            System.out.println("The response code is: " + response_code);

        } while(response_code != 200);

        System.out.println("\nSuccessfully sent the info of DC to LG!\n");
    }

    /*
    * Get the Test ID
    */
    private static String getTestID(String dns_lg) throws Exception {

        String url_log = "http://" + dns_lg + "/log";
        String test_id;

        // Go to the test log overview page
        Thread.sleep(2000);
        System.out.println("Getting the test log overview page: " + url_log);

        HttpGet httpGet = new HttpGet(url_log);
        CloseableHttpResponse httpResponse = new DefaultHttpClient().execute(httpGet);

        String buffer = EntityUtils.toString(httpResponse.getEntity());

        test_id = buffer.substring(buffer.indexOf("test."), buffer.indexOf("'>test."));

        return test_id;
    }

    /*
     * Get rps from the test log
     */
    private static double getRPSSum(String dns_lg, String test_id) throws Exception {

        String url_test = "http://" + dns_lg + "/log?name=" + test_id;
        double rps_sum = 0.0;

        System.out.println("Trying to go to the test log page, TestID: " + test_id);
        System.out.println("Sending HTTPRequest to URL of Log: " + url_test);

        HttpGet httpGet = new HttpGet(url_test);
        CloseableHttpResponse httpResponse = new DefaultHttpClient().execute(httpGet);

        String buffer = EntityUtils.toString(httpResponse.getEntity());
        System.out.println(buffer);

        // Have no result now, need to wait
        if (!buffer.contains("[Minute")) {
            System.out.println("No result now, wait for next round");
            return 0.0;
        }

        String[] lines = buffer.split("Minute");

        // Search from back to front to get the info of last minute
        String line = lines[lines.length-1];
        String[] l = line.split("=");

        for (int i = 1; i < l.length; i++) {
            String ll = l[i];
            int end = ll.indexOf("dc");
            String temp;
            if (end < 0) {
                temp = ll.substring(0, ll.length());
            } else {
                temp = ll.substring(0, end);
            }
            rps_sum += Double.parseDouble(temp);
        }

        return rps_sum;
    }


    /*
     * Main function
     */
    public static void main(String args[]) throws Exception {

        // Number of arguments
        if (args.length < 2) {
            System.err.println("Too Few Arguments!");
            return;
        }

        // Get submission info
        andrew_id = args[0];
        submission_id = args[1];

        // Get AWS Credentials
        Instance instance;
        Properties properties = new Properties();
        properties.load(RunInstance.class.getResourceAsStream("/AwsCredentials.properties"));
        BasicAWSCredentials aws_credential = new BasicAWSCredentials(properties.getProperty("accessKey"), properties.getProperty("secretKey"));
        
        // Create EC2 Client
        AmazonEC2Client ec2 = new AmazonEC2Client(aws_credential);
        RunInstance RunInstance = new RunInstance();
        String security_group = runInstance.getSecurityGroup(ec2);

        // Creating Load Generator
        num_lg += step;
        String[] para_lg = {ec2, ami_lg, instance_lg, "15619P2.1", security_group, tag_key, tag_value};
        System.out.println("Begin Creating Load Generator, No: " + num_lg);
        instance = runInstance.createInstance(para_lg);
        dns_lg = instance.getInstanceId();
        System.out.println("Successfully Created Load Generator!");
        System.out.println("The DNS of LG is: " + dns_lg + "\n");

        // Creating Data Center
        num_dc += step;
        String[] para_dc = {ec2, ami_dc, instance_dc, "15619P2.1", security_group, tag_key, tag_value};
        System.out.println("Begin Creating Data Center, No: " + num_dc);
        instance = runInstance.createInstance(para_dc);
        String dns_dc = instance.getInstanceId();
        System.out.println("Successfully Created Data Center!");
        System.out.println("The DNS of DC is: " + dns_dc + "\n");

        // Basic Setup
        basicSetup(dns_lg);
        Thread.sleep(40000);
        setupDataCenter(dns_dc, dns_lg, true);

        // Get Test ID
        String test_id = getTestID(dns_lg);
        System.out.println("Test_id: " + test_id);

        // Add instances till rps is no smaller than 3000
        System.out.println("\nNow RPS test begin!");
        double rps_sum = 0.0;
        do {
            // Add Data Center
            System.out.println("Begin Creating Data Center, No: " + num_dc);
            num_dc += step;
            instance = runInstance.createInstance(para_dc);
            String dns_dc = instance.getInstanceId();
            System.out.println("Successfully Created Data Center!");
            System.out.println("The DNS of DC is: " + dns_dc + "\n");

            // Wait to finish adding
            Thread.sleep(40000);
            setupDataCenter(dns_dc, dns_lg, false);

            // Get rps_sum
            rps_sum = getRPSSum(dns_lg, test_id);
            System.out.println("rps_sum is : " + rps_sum);

            Thread.sleep(60000);
        } while (rps_sum < 4000);

    }
}
