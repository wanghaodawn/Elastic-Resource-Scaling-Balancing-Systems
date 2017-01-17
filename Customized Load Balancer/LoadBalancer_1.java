/*
 * Load Balancer Using Round-Robin for Task 1
 *
 * Hao Wang - haow2
 *
*/

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadBalancer {
        private static final int THREAD_POOL_SIZE = 4;
        private final ServerSocket socket;
        private final DataCenterInstance[] instances;
        private static final int INSTANCE_NUM = 3;

        public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
                this.socket = socket;
                this.instances = instances;
        }

        /*
         *  Serves as the main function for Load Balancer
         */
        public void start() throws IOException {
                
                ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
                int i = 0; // Decide which DC to use

                while (true) {

                        if (i == INSTANCE_NUM) {
                                i = 0;
                        }

                        // By default, it will send all requests to the first instance
                        Runnable requestHandler = new RequestHandler(socket.accept(), instances[i]);
                        executorService.execute(requestHandler);
                        i++;

                }
        }
}
