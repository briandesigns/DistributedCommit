package LockManager;

import LockManager.DataObj;

/**
 * Created by brian on 30/10/15.
 */
public class Test {
    public static void main(String[] args) {
        DataObj dataobj = new DataObj(1, "a", LockManager.READ);

        XObj xobjHandle = (XObj) dataobj;
        TrxnObj trxnObj = new TrxnObj(1, "a", LockManager.READ);
        System.out.println(dataobj.hashCode());
        System.out.println(xobjHandle.hashCode());
        System.out.println(trxnObj.hashCode());

    }

}
