package PerformanceEvaluation;

import ResourceManager.Trace;

import java.io.*;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by brian on 10/11/15.
 */
public class TestClient extends ResourceManager.Client implements Runnable {
    private int waitTime = 0;
    private String oneClientType;
    private Random rand = new Random();

    public TestClient(String mwHost, int mwPort, String oneClientType) {
        super(mwHost, mwPort);
        this.oneClientType = oneClientType;
    }

    public TestClient(String mwHost, int mwPort, int waitTime) {
        super(mwHost, mwPort);
        this.waitTime = waitTime;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (oneClientType == null) {
                    if (waitTime == 0) {
                        Thread.currentThread().sleep((long) (waitTime));
                    } else {
                        Thread.currentThread().sleep((long) (waitTime - (0.10 * rand.nextInt(waitTime)) + (0.10 * rand.nextInt(waitTime))));

                    }
                    switch (rand.nextInt(3)) {
                        case 0:
                            transaction1(rand.nextInt(4));
                            break;
                        case 1:
                            transaction2(rand.nextInt(4));
                            break;
                        case 2:
                            transaction3(rand.nextInt(4));
                            break;
                        default:
                            break;
                    }
                } else {
                    if (oneClientType.equalsIgnoreCase("single")) singleRMTransaction(rand.nextInt(3));
                    else multiRMTransaction(rand.nextInt(2));
                }


            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void transaction1(int choice) {
        try {
            long startTime = System.currentTimeMillis();
            toMW.println("start");
            String response = fromMW.readLine();
            response = response.replaceAll("[\\D]", "");
            int XID = Integer.parseInt(response);

            int itemID = rand.nextInt(1000) + 1;
            switch (choice) {
                case 0:
                    toMW.println("newroom" + "," + XID + "," + itemID + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000 + 1)));
                    fromMW.readLine();
                    break;
                case 1:
                    toMW.println("queryroom" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    break;
                case 2:
                    toMW.println("queryroomprice" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    break;
                case 3:
                    toMW.println("deleteroom" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    break;
            }
            toMW.println("commit");
            fromMW.readLine();
            long endTime = System.currentTimeMillis();
            long responseTime = endTime - startTime;
            DataStorage.addResponseTime(responseTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void transaction2(int choice) {
        try {
            long startTime = System.currentTimeMillis();
            toMW.println("start");
            String response = fromMW.readLine();
            response = response.replaceAll("[\\D]", "");
            int XID = Integer.parseInt(response);
            int itemID = rand.nextInt(1000) + 1;
            switch (choice) {
                case 0:
                    toMW.println("newcar" + "," + XID + "," + itemID + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000 + 1)));
                    fromMW.readLine();
                    break;
                case 1:
                    toMW.println("querycar" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    break;
                case 2:
                    toMW.println("querycarprice" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    break;
                case 3:
                    toMW.println("deletecar" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    break;
            }
            toMW.println("commit");
            fromMW.readLine();
            long endTime = System.currentTimeMillis();
            long responseTime = endTime - startTime;
            DataStorage.addResponseTime(responseTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void transaction3(int choice) {
        try {
            long startTime = System.currentTimeMillis();
            toMW.println("start");
            String response = fromMW.readLine();
            response = response.replaceAll("[\\D]", "");
            int XID = Integer.parseInt(response);
            int itemID = rand.nextInt(1000) + 1;

            switch (choice) {
                case 0:
                    toMW.println("newflight" + "," + XID + "," + itemID + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000 + 1)));
                    fromMW.readLine();
                    break;
                case 1:
                    toMW.println("queryflight" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    break;
                case 2:
                    toMW.println("queryflightprice" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    break;
                case 3:
                    toMW.println("deleteflight" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    break;
            }
            toMW.println("commit");
            fromMW.readLine();
            long endTime = System.currentTimeMillis();
            long responseTime = endTime - startTime;
            DataStorage.addResponseTime(responseTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void singleRMTransaction(int RMchoice) {
        switch (RMchoice) {
            case 0:
                try {
                    long startTime = System.currentTimeMillis();
                    toMW.println("start");
                    String response = fromMW.readLine();
                    response = response.replaceAll("[\\D]", "");
                    int XID = Integer.parseInt(response);
                    int itemID = rand.nextInt(1000) + 1;
                    toMW.println("newroom" + "," + XID + "," + itemID + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000 + 1)));
                    fromMW.readLine();
                    toMW.println("queryroom" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    toMW.println("queryroomprice" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    toMW.println("deleteroom" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    toMW.println("commit");
                    fromMW.readLine();
                    long endTime = System.currentTimeMillis();
                    long responseTime = endTime - startTime;
                    DataStorage.addResponseTime(responseTime);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case 1:
                try {
                    long startTime = System.currentTimeMillis();
                    toMW.println("start");
                    String response = fromMW.readLine();
                    response = response.replaceAll("[\\D]", "");
                    int XID = Integer.parseInt(response);

                    int itemID = rand.nextInt(1000) + 1;
                    toMW.println("newCar" + "," + XID + "," + itemID + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000 + 1)));
                    fromMW.readLine();
                    toMW.println("queryCar" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    toMW.println("querycarPrice" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    toMW.println("deleteCar" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    toMW.println("commit");
                    fromMW.readLine();
                    long endTime = System.currentTimeMillis();
                    long responseTime = endTime - startTime;
                    DataStorage.addResponseTime(responseTime);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case 2:
                try {
                    long startTime = System.currentTimeMillis();
                    toMW.println("start");
                    String response = fromMW.readLine();
                    response = response.replaceAll("[\\D]", "");
                    int XID = Integer.parseInt(response);
                    int itemID = rand.nextInt(1000) + 1;
                    toMW.println("newflight" + "," + XID + "," + itemID + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000 + 1)));
                    fromMW.readLine();
                    toMW.println("queryflight" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    toMW.println("queryflightprice" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    toMW.println("deleteflight" + "," + XID + "," + itemID);
                    fromMW.readLine();
                    toMW.println("commit");
                    fromMW.readLine();
                    long endTime = System.currentTimeMillis();
                    long responseTime = endTime - startTime;
                    DataStorage.addResponseTime(responseTime);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            default:
                break;
        }
    }

    private void multiRMTransaction(int operationChoice) {
        switch (operationChoice) {
            case 0:
                try {
                    long startTime = System.currentTimeMillis();
                    toMW.println("start");
                    String response = fromMW.readLine();
                    response = response.replaceAll("[\\D]", "");
                    int XID = Integer.parseInt(response);
                    toMW.println("newcustomer," + "," + XID);
                    fromMW.readLine();
                    toMW.println("newflight" + "," + XID + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000) + 1));
                    fromMW.readLine();
                    toMW.println("newcar" + "," + XID + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000) + 1) + (rand.nextInt(1000) + 1));
                    fromMW.readLine();
                    toMW.println("newroom" + "," + XID + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000) + 1) + (rand.nextInt(1000) + 1));
                    fromMW.readLine();
                    toMW.println("commit");
                    fromMW.readLine();
                    long endTime = System.currentTimeMillis();
                    long responseTime = endTime - startTime;
                    DataStorage.addResponseTime(responseTime);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case 1:
                try {
                    long startTime = System.currentTimeMillis();
                    toMW.println("start");
                    String response = fromMW.readLine();
                    response = response.replaceAll("[\\D]", "");
                    int XID = Integer.parseInt(response);
                    toMW.println("newcustomer," + "," + XID);
                    int customerID = Integer.parseInt(fromMW.readLine());
                    toMW.println("itinerary" + "," + XID + "," + customerID + "," + (rand.nextInt(1000) + 1) + "," + (rand.nextInt(1000) + 1) + "," + "true" + "," + "true");
                    fromMW.readLine();
                    toMW.println("commit");
                    fromMW.readLine();
                    long endTime = System.currentTimeMillis();
                    long responseTime = endTime - startTime;
                    DataStorage.addResponseTime(responseTime);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            default:
                break;
        }
    }


}
