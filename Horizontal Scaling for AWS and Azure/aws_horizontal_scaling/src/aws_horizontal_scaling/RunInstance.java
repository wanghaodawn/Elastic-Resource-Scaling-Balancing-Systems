package aws_horizontal_scaling;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.*;

public class RunInstance {
	
    private static String tag_key = "Project";
	private static String tag_value = "2.1";
	private static String key_name = "15619demo";
	
	/*
	 * Create instance and return its DNS
	 */
	public static String createInstance 
	(AmazonEC2Client ec2, String security_group, String ami, String instance_type) throws Exception {
    	
		// Create instance request
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

        // Configure instance request
        runInstancesRequest.withImageId(ami)
                .withInstanceType(instance_type)
                .withMinCount(1)
                .withMaxCount(1)
                .withKeyName(key_name)
                .withSecurityGroups(security_group);

        // Launch instance
        RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);

        // Get instance id
        Instance instance = runInstancesResult.getReservation().getInstances().get(0);
        System.out.println("Just launched an instance with id: " + instance.getInstanceId());
        String instance_id = instance.getInstanceId();
        
        // Add tags
        CreateTagsRequest createTagsRequest = new CreateTagsRequest();
        createTagsRequest.withResources(instance.getInstanceId()).withTags(new Tag(tag_key,tag_value));
        ec2.createTags(createTagsRequest);
        
        // Get DNS
        String dns = "";
        do {
        	Thread.sleep(5000);
        	
        	// Get DNS, referenced from stack overflow
        	DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
            List<Reservation> reservations = describeInstancesRequest.getReservations();

            for (Reservation reservation : reservations) {
              for (Instance instance0 : reservation.getInstances()) {
                if (instance0.getInstanceId().equals(instance_id))
                  return instance0.getPublicDnsName();
              }
            }
        	
        } while(dns == null);
        
        return dns;
    }
}


