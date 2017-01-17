import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created by Dawn on 16/2/6.
 */
public class union_test {
    public static void main(String[] args) throws Exception {
        File inFile = new File("/Users/Dawn/Desktop/Git/15619-Cloud-Computing/P2_Scaling_Elasticity_and_Failure/P2.1_Horizontal_Scaling_and_Advanced_Resource_Scaling/azureDemo/src/main/java/test");

        BufferedReader br = null;
        String s = "";

        String sCurrentLine;
        br = new BufferedReader(new FileReader(inFile));

        while ((sCurrentLine = br.readLine()) != null) {
            //System.out.println(sCurrentLine);
            s += sCurrentLine;
        }

        System.out.println(s);

        String[] lines = s.split("Minute");

        // Search from back to front to get the info of last minute
        String line = lines[lines.length-1];
        System.out.println(line);

        String[] l = line.split("=");
        System.out.println(l.length);

        double sum = 0.0;
        for (int i = 1; i < l.length; i++) {
            String ll = l[i];
//            System.out.println(ll);
            int end = l[i].indexOf("dc");
//            System.out.println(end);
            String temp;
            if (end < 0) {
                temp = ll.substring(0, ll.length());
            } else {
                temp = ll.substring(0, end);
            }

            sum = Double.parseDouble(temp);
            System.out.println(sum);
        }

//        System.out.println(sum);

        br.close();
    }
}
