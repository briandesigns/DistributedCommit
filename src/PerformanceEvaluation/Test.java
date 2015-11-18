package PerformanceEvaluation;

import java.util.Scanner;

/**
 * Created by brian on 11/11/15.
 */
public class Test {
    public static void main(String[] args) {
        String s = "transaction started with XID: 14";
        System.out.println(Integer.parseInt(s.replaceAll("[\\D]", "")));

    }
}
