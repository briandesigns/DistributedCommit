package PerformanceEvaluation;

import ResourceManager.Trace;
import TransactionManager.RMTimeOutException;

import java.util.Scanner;
import java.util.concurrent.*;

/**
 * Created by brian on 11/11/15.
 */
public class Test {
    public static void main(String[] args) {
        ExecutorService es = Executors.newSingleThreadExecutor();
        Future future = es.submit(new RMtimouter());
        try {
            future.get(6000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            Trace.error("timed out");
        } finally {
            es.shutdown();
        }

        System.out.println("hello");
//        es.shutdownNow();

    }
}

class RMtimouter implements Callable<Boolean> {

    @Override
    public Boolean call() throws Exception {
        Thread.currentThread().sleep(5000);

        return true;
    }
}
