/*
 *
 * Azure Horizontal Scaling
 *
 * by Hao Wang - haow2
 *
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
    private static int step = 10;

    // Stores the information
    private static String res_group = "";
    private static String storage_acc = "";
    private static String image_lg = "cc15619p21lgv10-osDisk.f6be8828-8cab-45ae-a611-904aeeef3c9e.vhd";
    private static String image_dc = "cc15619p21dcv5-osDisk.e27faca3-f177-40ea-a740-9a1838326ae6.vhd";
    private static String subscription_id = "";
    private static String tenant_id = "";
    private static String app_id = "";
    private static String app_key = "";
    private static String instance_a1 = "Standard_A1";
    private static String instance_d1 = "Standard_D1";
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
        if (args.length < 3) {
            System.err.println("Too Few Arguments!");
            return;
        }

        // Configuration
        res_group = "haowangrg";
        storage_acc = "haowangsa";
        subscription_id = args[0];
        tenant_id = "0e5a22ea-a170-469a-9bc9-2a8991cdd49c";
        app_id = "14cd5288-f981-4eea-a90e-77cf9eb058d1";
        app_key = args[1];
        andrew_id = "haow2";
        submission_id = args[2];

        // Creating Load Generator
        num_lg += step;
        String[] para_lg = {res_group, storage_acc, image_lg, subscription_id,
                            tenant_id, app_id, app_key, instance_d1, ""+ num_lg};
        System.out.println("Begin Creating Load Generator, No: " + num_lg);
        String dns_lg = AzureVMApiDemo.main(para_lg);
        System.out.println("Successfully Created Load Generator!");
        System.out.println("The DNS of LG is: " + dns_lg + "\n");

        // Creating Data Center
        num_dc += step;
        String[] para_dc = {res_group, storage_acc, image_dc, subscription_id,
                            tenant_id, app_id, app_key, instance_a1, ""+ num_dc};
        System.out.println("Begin Creating Data Center, No: " + num_dc);
        String dns_dc = AzureVMApiDemo.main(para_dc);
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
            num_dc += step;
            para_dc[8] = ""+num_dc;
            dns_dc = AzureVMApiDemo.main(para_dc);

            // Wait to finish adding
            Thread.sleep(40000);
            setupDataCenter(dns_dc, dns_lg, false);

            // Get rps_sum
            rps_sum = getRPSSum(dns_lg, test_id);
            System.out.println("rps_sum is : " + rps_sum);

            Thread.sleep(60000);
        } while (rps_sum < 3000);

    }
}
