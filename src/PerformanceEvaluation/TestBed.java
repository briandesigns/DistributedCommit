package PerformanceEvaluation;

import java.util.ArrayList;

/**
 * Created by brian on 11/11/15.
 */
public class TestBed {
    static ArrayList<Thread> clients = new ArrayList<Thread>();
    static Thread TestAClient;
    static final int NUMBER_OF_CLIENTS = 1000;
    public static void main(String[] args) {
        try {

            if (args.length < 2) {
                System.out.println("Usage: ResourceManager.Client <mwHost> <mwPort> <a|b> (<waitTimeMS>)");
                System.exit(-1);
            }

            String mwHost = args[0];
            int mwPort = Integer.parseInt(args[1]);
            final String testType = args[2];
             int waitTime=0;
            if (!(args[3] == null)) {
                waitTime = Integer.parseInt(args[3]);
            }
            int testTime =0;
            if (!(args[4] == null)) {
                testTime = Integer.parseInt(args[4]);
            }
            if (testType.equalsIgnoreCase("a")) {
                startTestAPart1(mwHost, mwPort, testTime);
            } else if (testType.equalsIgnoreCase("b")) {
                startTestB(mwHost, mwPort, waitTime, testTime);
            } else {
                System.out.println("wrong test type");
                System.exit(0);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startTestB(String mwHost, int mwPort, int waitTime, final int testTime) {
        final int time = waitTime;
        for (int i = 0; i<NUMBER_OF_CLIENTS; i++) {
            clients.add(new Thread(new TestClient(mwHost, mwPort, time)));
        }
        for (int i=0; i<NUMBER_OF_CLIENTS; i++) {
            clients.get(i).start();
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().sleep(testTime);
                    endTestB("b_" + time);
                    System.out.println("end test b called");
                    System.exit(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();


    }

    private static void startTestAPart1(final String mwHost, final int mwPort, final int testTime) {
        TestAClient = new Thread(new TestClient(mwHost,mwPort,"single"));
        TestAClient.start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().sleep(testTime);
                    endTestAPart1("a_singleRM");
                    System.out.println("test a part 1 ended");
                    startTestAPart2(mwHost, mwPort, testTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static void startTestAPart2(String mwHost, int mwPort, final int testTime) {
        TestAClient = new Thread(new TestClient(mwHost, mwPort,"multi"));
        TestAClient.start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().sleep(testTime);
                    endTestAPart2("a_multiRM");
                    System.out.println("test a part 2 ended");
                    System.exit(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private static void endTestB(String testType) {
        for (int i = 0; i<clients.size(); i++ ) {
            clients.get(i).interrupt();
        }
        DataStorage.writeDataToFile(testType);
    }
    private static void endTestAPart1(String testType) {
            TestAClient.interrupt();
            DataStorage.writeDataToFile(testType);

    }

    private static void endTestAPart2(String testType) {
        TestAClient.interrupt();
        DataStorage.writeDataToFile(testType);

    }
}
