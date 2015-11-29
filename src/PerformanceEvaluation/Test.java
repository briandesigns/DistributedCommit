package PerformanceEvaluation;

import TransactionManager.RMTimeOutException;

import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by brian on 11/11/15.
 */
public class Test {
    public static void main(String[] args) {
        ExecutorService es = Executors.newSingleThreadExecutor();
        es.submit(new RMtimouter());
        System.out.println("hello");
//        es.shutdownNow();

    }
}

class RMtimouter implements Callable<RMTimeOutException> {

    @Override
    public RMTimeOutException call() throws Exception {
        Thread.currentThread().sleep(5000);
        System.out.println("hello");
        return new RMTimeOutException("hello");
    }
}
