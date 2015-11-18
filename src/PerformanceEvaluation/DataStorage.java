package PerformanceEvaluation;

import ResourceManager.Trace;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by brian on 11/11/15.
 */
public class DataStorage {
    static private ArrayList<Long> responseTimes = new ArrayList<Long>();

    public static synchronized void addResponseTime(long responseTime) {
        responseTimes.add(responseTime);
        System.out.println("response time added");
    }

    public static void writeDataToFile(String testType) {
        try {
            String path = TestClient.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            path = new File(path).getParent();
            path = path + "/"+testType +"_response_times.csv";
            File file = new File(path);
            PrintWriter writer = new PrintWriter(file, "UTF-8");

            for (int i=0; i<responseTimes.size(); i++) {
                writer.print(responseTimes.get(i) + ",");
            }
            writer.println("\n");
            writer.println("count:" + responseTimes.size());
            writer.println("average:" + calculateAverage());
            writer.close();
            System.out.println("response time array size: " + responseTimes.size());
            responseTimes = new ArrayList<Long>();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Trace.info("Cannot find RMList.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static float calculateAverage() {
        float result = 0f;
        for (int i = 0; i < responseTimes.size(); i++) {
            result+=responseTimes.get(i);
        }
        result/=responseTimes.size();
        return result;

    }

}
