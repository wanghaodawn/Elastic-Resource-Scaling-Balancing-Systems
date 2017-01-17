/*
 * Load Balancer Combined Round-Robin and CPU Utilization - Task 2
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


public class LoadBalancer {
        private static final int THREAD_POOL_SIZE = 4;
        private final ServerSocket socket;
        private final DataCenterInstance[] instances;
        private static final int INSTANCE_NUM = 3;
        private static int min_util_index = 0;

        public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
                this.socket = socket;
                this.instances = instances;
        }

        /*
         *  Serves as the main function for Load Balancer
         */
        public void start() throws IOException {
                
                ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
                int instance_index = 0;
                int left_times = 9; // Check CPU status every several requests
                int i = 0;

                while (true) {

                        if (i == 3) {
                                i = 0;
                        }

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

                                // By default, it will send all requests to the first instance
                                Runnable requestHandler = new RequestHandler(socket.accept(), instances[i]);
                                executorService.execute(requestHandler);

                        } else {
                                left_times--;

                                // By default, it will send all requests to the first instance
                                Runnable requestHandler = new RequestHandler(socket.accept(), instances[i]);
                                executorService.execute(requestHandler);
                        }
                        i++;

                }
        }

        /*
         * Get the CPU Utilization for each instances and the DC that has the lowest CPU Utilization
        */
        private double[] parseDCCPU() throws IOException {

                min_util_index = 0;
                double min_util = Double.MAX_VALUE;               

                double[] cpu_util  = new double[INSTANCE_NUM];
                for (int i = 0; i < INSTANCE_NUM; i++) {

                        // Get the ulr of the api
                        String url = instances[i].getUrl() + ":8080/info/cpu";
                        // System.out.println(url);

                        // Get the content of the api website to get cpu util
                        URL obj = new URL(url);
                        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

                        con.setRequestMethod("GET");
                        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));

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
                
                return cpu_util;
        }
}