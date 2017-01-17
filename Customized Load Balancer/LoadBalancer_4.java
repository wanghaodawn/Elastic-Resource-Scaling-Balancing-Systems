/*
 * Load Balancer Customed Strategy and Monitored the Health of Data Center - Task 4
 *
 * Hao Wang - haow2
 *
*/

import java.io.IOException;
import java.io.*;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.*;


public class LoadBalancer {
        private static final int THREAD_POOL_SIZE = 4;
        private final ServerSocket socket;
        private DataCenterInstance[] instances;
        private static final int INSTANCE_NUM = 3;
        private static int min_util_index = 0;

        // To Keep it Thread Safe
        private static Vector<Integer> dead_dc_index;

        // Record the number of dc and lg
        private static int num_dc = 10000;
        // The ratio to control the name to prevent identical name
        private static int step = 1;

        // Stores the information
        public static String res_group = "haowangrg";
        public static String storage_acc = "haowangsa";
        public static String image_dc = "cc15619p22dcv6-osDisk.b0c453f3-f75f-4a2d-bd9c-ae055b830124.vhd";
        public static String subscription_id = "";
        public static String tenant_id = "0e5a22ea-a170-469a-9bc9-2a8991cdd49c";
        public static String app_id = "14cd5288-f981-4eea-a90e-77cf9eb058d1";
        public static String app_key = "";
        public static String instance_dc = "Standard_A1";

        public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
                this.socket = socket;
                this.instances = instances;
        }

        /*
         *  Serves as the main function for Load Balancer
         */
        public void start() throws IOException, InterruptedException {
                
                System.out.println("Begin Start");
                ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
                int i = 0; // Decide which DC to use
                getArgs();
                int left_times = 9;
                dead_dc_index = new Vector<Integer>();
                int check_health_wait = 45; // Check health of a DC every several loops

                System.out.println("Now will enter the loop");

                while (true) {

                        if (i == INSTANCE_NUM) {
                                i = 0;
                        }

                        // Skip dead data centers
                        boolean flag = false;
                        for (int j = 0; j < dead_dc_index.size(); j++) {
                                if (i == dead_dc_index.get(j)) {
                                        i++;
                                        flag = true;
                                        break;
                                }
                        }

                        if (flag) {
                                continue;
                        }

                        // System.out.println("i: " + i);

                        if (left_times == 0) {
                                // Get the CPU Rate now
                                double[] cpu_util  = parseDCCPU();

                                if (cpu_util == null) {
                                        // Have problem now
                                        continue;
                                }

                                // Find the CPU with lowest util rate and its index
                                i = min_util_index;
                                left_times = 9;
                                // System.out.println("i: " + min_util_index + "min: " + min_util);

                                // System.out.println("left_times: " + left_times);

                                
                                // By default, it will send all requests to the first instance
                                Runnable requestHandler = new RequestHandler(socket.accept(), instances[i]);
                                executorService.execute(requestHandler);

                        } else {
                                left_times--;
                                // System.out.println("left_times: " + left_times);
                                // By default, it will send all requests to the first instance
                                Runnable requestHandler = new RequestHandler(socket.accept(), instances[i]);
                                executorService.execute(requestHandler);
                        }
                        i++;

                        // Check health
                        // System.out.println("check_health_wait: " + check_health_wait);

                        if (check_health_wait == 0) {
                                checkHealth();
                                check_health_wait = 45;
                        } else {
                                check_health_wait--;
                        }
                        // System.out.println("check_health_wait: " + check_health_wait);

                }
        }

        /*
         * Create a new thread to launch Data Center
         */
        public class launchDC extends Thread {

                public void run() {

                        try {
                                System.out.println("Creating new DC: " + dead_dc_index.get(0));
                                // Create DC
                                num_dc += step;
                                String[] para_dc1 = {res_group, storage_acc, image_dc, subscription_id,
                                                     tenant_id, app_id, app_key, instance_dc, ""+ num_dc};
                                System.out.println("Begin Creating Data Center, No: " + num_dc);
                                String url_dc = "http://" + AzureVMApiDemo.main(para_dc1);
                                System.out.println("Successfully Created Data Center!");
                                System.out.println("The URL of DC is: " + url_dc + "\n");

                                String name_dc = instances[dead_dc_index.get(0)].getName();

                                Thread.sleep(40000);

                                // Check Status
                                URL url = new URL(url_dc);
                                int response_code;
                                HttpURLConnection url_conn;

                                // Make sure the new DC is ready
                                do {
                                    try {
                                        Thread.sleep(7000);
                                        url_conn = (HttpURLConnection) url.openConnection();
                                        response_code = url_conn.getResponseCode();
                                        System.out.println("The response code is: " + response_code);
                                    } catch (Exception e) {
                                        response_code = 0;
                                        System.out.println("The response code is: " + response_code);
                                    }
                                } while (response_code != 200);

                                // Substitute bad DC
                                instances[dead_dc_index.get(0)] = new DataCenterInstance(name_dc, url_dc);
                                System.out.println("Finished creating new DC: " + dead_dc_index.get(0));
                                dead_dc_index.remove(0);

                        } catch (Exception e) {
                                e.printStackTrace();
                        }
                        
                }
        }

        /*
         * Check the health of a data center
         */
        private void checkHealth() throws IOException, InterruptedException {
                // System.out.println("Now Checking Health");

                for (int i = 0; i < dead_dc_index.size(); i++) {
                    System.out.println("dead_dc_index i: " + i + " dns: " + instances[dead_dc_index.get(i)].getUrl());
                }

                for (int i = 0; i < INSTANCE_NUM; i++) {

                        // Skip dead Data Centers
                        if (dead_dc_index.contains(i)) {
                            continue;
                        }

                        // Check Status
                        String url_temp = instances[i].getUrl() + "/lockup/random";
                        URL url = new URL(url_temp);
                        int response_code;
                        HttpURLConnection url_conn;
                        boolean isCrashed = false;

                        // Check each DC in case of making mistakes
                        try{
                            url_conn = (HttpURLConnection) url.openConnection();
                            response_code = url_conn.getResponseCode();
                            // System.out.println("This Time, The response code is: " + response_code);
                            
                            if (response_code == 200) {
                                    continue;
                            }
                            isCrashed = true;

                        } catch (IOException e) {
                            System.out.println("No Result Returned");
                            isCrashed = true;
                        }

                        // Found Failure
                        if (isCrashed) {
                                System.out.println("DC No: " + i + "Failed, URL: " + url_temp);
                                if (!dead_dc_index.contains(i)) {
                                        dead_dc_index.add(i);
                                        // Treat the data center as dead so create a new one to replace
                                        launchDC launch = new launchDC();
                                        launch.start();
                                }
                        }
                        
                }
                // System.out.println("Finished Checking Health");
        }

        /*
         * Get the CPU Utilization for each instances and the DC that has the lowest CPU Utilization
        */
        private double[] parseDCCPU() throws IOException {

                // System.out.println("Now Getting CPU Utilization");
                min_util_index = 0;
                double min_util = Double.MAX_VALUE;               

                double[] cpu_util  = new double[INSTANCE_NUM];
                for (int i = 0; i < INSTANCE_NUM; i++) {

                        // Skip dead data centers
                        boolean flag = false;
                        for (int j = 0; j < dead_dc_index.size(); j++) {
                                if (i == dead_dc_index.get(j)) {
                                        i++;
                                        flag = true;
                                        break;
                                }
                        }

                        if (flag) {
                                continue;
                        }

                        // Get the ulr of the api
                        String url_i = instances[i].getUrl() + ":8080/info/cpu";
                        // System.out.println(url_i);

                        // Get the content of the api website to get cpu util
                        URL url = new URL(url_i);
                        HttpURLConnection url_con = (HttpURLConnection) url.openConnection();

                        BufferedReader in = new BufferedReader(new InputStreamReader(url_con.getInputStream()));

                        String line, buffer = "";
                        while ((line = in.readLine()) != null) {
                                buffer += line;
                        }
                        // System.out.println(buffer);

                        if (buffer == "") {
                                continue;
                        }

                        // Get the util number
                        int left = buffer.indexOf("<body>") + 6;
                        int right = buffer.indexOf("</body>");
                        // System.out.println("left: " + left + "right: " + right);
                        if (left != -1 && right != -1) {
                                String temp = buffer.substring(left, right);
                                if (temp == null || temp.length() == 0) {
                                        continue;
                                }
                                cpu_util[i] = Double.parseDouble(temp);

                                if (cpu_util[i] < min_util) {
                                        min_util = cpu_util[i];
                                        min_util_index = i;
                                }

                                // System.out.println("i: " + i + " util: " + cpu_util[i]);
                        } else {
                                // System.out.println("Illegal number");
                                return null;
                        }
                }

                // System.out.println("Finished Getting CPU Utilization");
                return cpu_util;
        }

        /*
         * Get the credentials from file
         */
        private void getArgs() throws IOException {

                System.out.println("Begin getting arguments");
                // Read credentials from file
                File inFile = new File("credentials");
                
                // If file doesnt exists, then create it
                if (!inFile.exists()) {
                    System.err.println("No file called: credentials");
                    System.exit(-1);
                }

                BufferedReader br = null;
                String buffer = "";

                // Read string from the input file
                String line = "";
                
                br = new BufferedReader(new FileReader(inFile));

                while ((line = br.readLine()) != null) {
                    //System.out.println(sCurrentLine);
                    buffer += line;
                }

                String[] ss = buffer.split("\t");

                if (ss == null || ss.length != 2) {
                        System.err.println("credentials has wrong parameters");
                        System.exit(-1);
                }

                // Get three credential arguments
                subscription_id = ss[0];
                app_key = ss[1];

                br.close();

                // Print to check
                System.out.println("subscription_id: " + subscription_id);
                System.out.println("app_key: " + app_key);
                System.out.println("Finshed Getting Arguments");
        }
}