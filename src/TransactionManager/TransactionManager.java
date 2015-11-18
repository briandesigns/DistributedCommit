package TransactionManager;

import ResourceManager.*;
import LockManager.*;
import LockManager.LockManager;
import ResourceManager.TCPServer;
import ResourceManager.ResourceManager;
import ResourceManager.Customer;
import ResourceManager.MiddlewareRunnable;

import java.io.IOException;
import java.util.*;

public class TransactionManager implements ResourceManager {
    public static Hashtable<Integer, boolean[]> transactionTable;
    public ArrayList<Customer> customers;
    private ArrayList<String> undoStack;
    private static int uniqueTransactionID = 0;
    private int currentActiveTransactionID;
    public static final int UNUSED_TRANSACTION_ID = -1;
    private MiddlewareRunnable myMWRunnable;
    private boolean inTransaction = false;
    private Thread TTLCountDownThread;
    private static final int TTL_MS = 60000;
    public static final String CAR = "car-";
    public static final String FLIGHT = "flight-";
    public static final String ROOM = "room-";
    public static final String CUSTOMER = "customer-";

    {
        transactionTable = new Hashtable<Integer, boolean[]>();
    }


    private void setInTransaction(boolean decision) {
        this.inTransaction = decision;
    }

    public int getCurrentActiveTransactionID() {
        return currentActiveTransactionID;
    }

    public boolean isInTransaction() {
        return inTransaction;
    }

    public void renewTTLCountDown() {
        TTLCountDownThread.interrupt();
        startTTLCountDown();
    }

    public void stopTTLCountDown() {
        TTLCountDownThread.interrupt();
    }

    private void startTTLCountDown() {
        TTLCountDownThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().sleep(TTL_MS);
                    abort();
                } catch (InterruptedException e) {
                    System.out.println("TTL renewed");
                }
            }
        });
        TTLCountDownThread.start();
    }


    public TransactionManager(MiddlewareRunnable myMWRunnable) {
        this.myMWRunnable = myMWRunnable;
        this.undoStack = new ArrayList<String>();
        this.customers = new ArrayList<Customer>();
    }

    public static synchronized boolean noActiveTransactions() {
        return transactionTable.isEmpty();
    }

    private static synchronized int generateUniqueXID() {
        uniqueTransactionID++;
        return uniqueTransactionID;
    }

    private boolean undoAll() {
        boolean result = true;
        for (int i = this.undoStack.size() - 1; i >= 0; i--) {
            String line = undoStack.get(i);
            executeUndoLine(line);
        }
        return result;
    }

    private boolean executeUndoLine(String line) {
        if (line.toLowerCase().contains("unreserve")) {
            String[] cmdWords = line.split(",");
            try {
                return myMWRunnable.unreserveItem(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]), (cmdWords[3]), (cmdWords[4]));
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        } else if (line.toLowerCase().contains("flight")) {
            myMWRunnable.toFlight.println(line);
            try {
                if (myMWRunnable.fromFlight.readLine().toLowerCase().contains("false"))
                    return false;
                else return true;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        } else if (line.toLowerCase().contains("car")) {
            myMWRunnable.toCar.println(line);
            try {
                if (myMWRunnable.fromCar.readLine().toLowerCase().contains("false"))
                    return false;
                else return true;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        } else if (line.toLowerCase().contains("room")) {
            myMWRunnable.toRoom.println(line);
            try {
                if (myMWRunnable.fromRoom.readLine().toLowerCase().contains("false"))
                    return false;
                else return true;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        } else if (line.toLowerCase().contains("undodeletecustomer")) {
            String[] cmdWords = line.split(",");
            return myMWRunnable.undoDeleteCustomer(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
        } else if (line.toLowerCase().contains("deletecustomer")) {
            String[] cmdWords = line.split(",");
            return myMWRunnable.deleteCustomer(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
        } else {
            return false;
        }

    }

    public boolean start() {
        if (!isInTransaction()) {
            startTTLCountDown();
            setInTransaction(true);
            currentActiveTransactionID = generateUniqueXID();
            System.out.println("transaction started with XID: " + currentActiveTransactionID);
            boolean[] RMInvolved = {false, false, false, false};
            TransactionManager.transactionTable.put(currentActiveTransactionID, RMInvolved);
            return true;
        } else {
            System.out.println("nothing to start, already in transaction");
            return false;
        }

    }

    public boolean abort() {
        stopTTLCountDown();
        setInTransaction(false);
        boolean result = undoAll();
        System.out.println("txn: " + this.currentActiveTransactionID + " call ended and undoAll() successful:" + result);
        transactionTable.remove(this.currentActiveTransactionID);
        this.undoStack = new ArrayList<String>();
        this.customers = new ArrayList<Customer>();
        TCPServer.lm.UnlockAll(currentActiveTransactionID);
        currentActiveTransactionID = UNUSED_TRANSACTION_ID;
        return result;
    }

    public boolean commit() {
        stopTTLCountDown();
        setInTransaction(false);
        transactionTable.remove(this.currentActiveTransactionID);
        this.undoStack = new ArrayList<String>();
        this.customers = new ArrayList<Customer>();
        TCPServer.lm.UnlockAll(this.currentActiveTransactionID);
        currentActiveTransactionID = UNUSED_TRANSACTION_ID;
        return true;
    }

    @Override
    public boolean addFlight(int id, int flightNumber, int numSeats, int flightPrice) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, FLIGHT+flightNumber, LockManager.WRITE)) {
                return false;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }
            String undoCmd;
            if (myMWRunnable.isExistingFlight(id, flightNumber)) {
                undoCmd = "undoAddFlight," + id + "," + flightNumber + "," + myMWRunnable.queryFlight(id, flightNumber) + "," + myMWRunnable.queryFlightPrice(id, flightNumber) + "," + myMWRunnable.queryFlightReserved(id, flightNumber);
            } else {
                undoCmd = "deleteFlight," + id + "," + flightNumber;
            }
            undoStack.add(undoCmd);

            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{true, false, false, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[0] = true;
            }
            return myMWRunnable.addFlight(id, flightNumber, numSeats, flightPrice);
        } catch (DeadlockException e) {
//            e.printStackTrace();
            System.out.println("TM ADDFLIGHT DEADLOCK JUST ABORTED, ABOUT TO RETURN FALSE");
            abort();
            return false;
        }
    }


    @Override
    public boolean deleteFlight(int id, int flightNumber) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, FLIGHT+flightNumber, LockManager.WRITE)) {
                return false;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }

            if (myMWRunnable.isExistingFlight(id, flightNumber)) {
                String undoCmd = "undoAddFlight," + id + "," + flightNumber + "," + myMWRunnable.queryFlight(id, flightNumber) + "," + myMWRunnable.queryFlightPrice(id, flightNumber) + "," + myMWRunnable.queryFlightReserved(id, flightNumber);
                undoStack.add(undoCmd);
            }
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{true, false, false, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[0] = true;
            }
            return myMWRunnable.deleteFlight(id, flightNumber);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return false;
        }
    }

    @Override
    public int queryFlight(int id, int flightNumber) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, FLIGHT+flightNumber, LockManager.READ)) {
                return -2;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return -3;
            }
            return myMWRunnable.queryFlight(id, flightNumber);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return -2;
        }
    }

    @Override
    public int queryFlightPrice(int id, int flightNumber) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, FLIGHT+flightNumber, LockManager.READ)) {
                return -2;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return -3;
            }
            return myMWRunnable.queryFlightPrice(id, flightNumber);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return -2;
        }
    }

    @Override
    public boolean addCars(int id, String location, int numCars, int carPrice) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, CAR+location, LockManager.WRITE)) {
                return false;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }
            String undoCmd;
            if (myMWRunnable.isExistingCars(id, location)) {
                undoCmd = "undoAddCars," + id + "," + location + "," + myMWRunnable.queryCars(id, location) + "," + myMWRunnable.queryCarsPrice(id, location) + "," + myMWRunnable.queryCarsReserved(id, location);
                System.out.println("just built undoCmd:" + undoCmd);
            } else {
                undoCmd = "deleteCars," + id + "," + location;
            }
            undoStack.add(undoCmd);

            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, true, false, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[1] = true;
            }
            return myMWRunnable.addCars(id, location, numCars, carPrice);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return false;
        }
    }

    @Override
    public boolean deleteCars(int id, String location) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, CAR+location, LockManager.WRITE)) {
                return false;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }
            if (myMWRunnable.isExistingCars(id, location)) {
                String undoCmd = "undoAddCars," + id + "," + location + "," + myMWRunnable.queryCars(id, location) + "," + myMWRunnable.queryCarsPrice(id, location) + "," + myMWRunnable.queryCarsReserved(id, location);
                undoStack.add(undoCmd);
            }
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, true, false, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[1] = true;
            }
            return myMWRunnable.deleteCars(id, location);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return false;
        }
    }

    @Override
    public int queryCars(int id, String location) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, CAR+location, LockManager.READ)) {
                return -2;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);
                return -3;
            }
            return myMWRunnable.queryCars(id, location);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return -1;
        }
    }

    @Override
    public int queryCarsPrice(int id, String location) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, CAR+location, LockManager.READ)) {
                return -2;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return -3;
            }
            return myMWRunnable.queryCarsPrice(id, location);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return -2;
        }
    }

    @Override
    public boolean addRooms(int id, String location, int numRooms, int roomPrice) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, ROOM+location, LockManager.WRITE)) {
                return false;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }
            String undoCmd;
            if (myMWRunnable.isExistingRooms(id, location)) {
                undoCmd = "undoAddRooms," + id + "," + location + "," + myMWRunnable.queryRooms(id, location) + "," + myMWRunnable.queryRoomsPrice(id, location) + "," + myMWRunnable.queryRoomsReserved(id, location);
            } else {
                undoCmd = "deleteRooms," + id + "," + location;
            }
            undoStack.add(undoCmd);
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, false, true, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[2] = true;
            }
            return myMWRunnable.addRooms(id, location, numRooms, roomPrice);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return false;
        }
    }

    @Override
    public boolean deleteRooms(int id, String location) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, ROOM+location, LockManager.WRITE)) {
                return false;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }
            if (myMWRunnable.isExistingRooms(id, location)) {
                String undoCmd = "undoAddRooms," + id + "," + location + "," + myMWRunnable.queryRooms(id, location) + "," + myMWRunnable.queryRoomsPrice(id, location) + "," + myMWRunnable.queryRoomsReserved(id, location);
                undoStack.add(undoCmd);
            }
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, false, true, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[2] = true;
            }
            return myMWRunnable.deleteRooms(id, location);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return false;
        }
    }

    @Override
    public int queryRooms(int id, String location) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, ROOM+location, LockManager.READ)) {
                return -2;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return -3;
            }
            return myMWRunnable.queryRooms(id, location);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return -2;
        }
    }

    @Override
    public int queryRoomsPrice(int id, String location) {
        try {
            renewTTLCountDown();
            if (!TCPServer.lm.Lock(id, ROOM+location, LockManager.READ)) {
                return -2;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return -3;
            }
            return myMWRunnable.queryRoomsPrice(id, location);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return -2;
        }
    }

    @Override
    public int newCustomer(int id) {
        renewTTLCountDown();
        if (this.currentActiveTransactionID != id) {
            System.out.println("transaction id does not match current transaction, command ignored");
            TCPServer.lm.UnlockAll(id);
            return -3;
        }
        int value = myMWRunnable.newCustomer(id);
        if (value == -2) {
            return -2;
        }

        String undoCmd = "deleteCustomer," + id + "," + value;
        undoStack.add(undoCmd);

        boolean[] involvedRMs = transactionTable.get(id);
        if (involvedRMs == null) {
            involvedRMs = new boolean[]{false, false, false, true};
            transactionTable.put(id, involvedRMs);
        } else {
            involvedRMs[3] = true;
        }
        return value;
    }

    @Override
    public boolean newCustomerId(int id, int customerId) {
        try {
            renewTTLCountDown();
            if (!(TCPServer.lm.Lock(id, CUSTOMER+customerId, LockManager.WRITE))) {
                return false;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);
                return false;
            }
            boolean success = myMWRunnable.newCustomerId(id, customerId);
            if (success) {
                String undoCmd = "deleteCustomer," + id + "," + customerId;
                System.out.println("just built undoCmd:" + undoCmd);
                undoStack.add(undoCmd);
            }
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, false, false, true};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[3] = true;
            }
            return success;
        } catch (DeadlockException e) {
//            e.printStackTrace();
            abort();
            return false;
        }
    }

    @Override
    public boolean deleteCustomer(int id, int customerId) {
        try {
            renewTTLCountDown();
            if (!(TCPServer.lm.Lock(id, CUSTOMER+customerId, LockManager.WRITE) )) {
                return false;
            }
            if (!myMWRunnable.getLockforCustomer(id, customerId)) return false;

            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);
                return false;
            }
            Customer cust = ((Customer) myMWRunnable.readData(id, Customer.getKey(customerId)));
            if (cust == null) {
                System.out.println("customer does not exist. failed to delete customer");
                return false;
            }
            customers.add(cust);
            String cmdWords = "undoDeleteCustomer" + "," + id + "," + customerId;
            undoStack.add(cmdWords);
            return myMWRunnable.deleteCustomer(id, customerId);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return false;
        }
    }


    @Override
    public String queryCustomerInfo(int id, int customerId) {
        try {
            renewTTLCountDown();
            if (!(TCPServer.lm.Lock(id, CUSTOMER+customerId, LockManager.READ))) {
                return "can't get customer Info";
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);
                return "transaction ID does not match current transaction, command ignored";
            }
            return myMWRunnable.queryCustomerInfo(id, customerId);
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            return "can't get customer Info";
        }
    }

    @Override
    public boolean reserveFlight(int id, int customerId, int flightNumber) {
        try {
            renewTTLCountDown();
            if (!(TCPServer.lm.Lock(id, CUSTOMER+customerId, LockManager.WRITE) && TCPServer.lm.Lock(id, FLIGHT+flightNumber, LockManager.WRITE))) {
                return false;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }
            if (myMWRunnable.reserveFlight(id, customerId, flightNumber)) {
                undoStack.add("unreserveItem" + "," + id + "," + customerId + "," + Flight.getKey(flightNumber) + "," + flightNumber);
                return true;
            } else return false;
        } catch (DeadlockException e) {
            abort();
            return false;
        }
    }

    @Override
    public boolean reserveCar(int id, int customerId, String location) {
        try {
            renewTTLCountDown();
            if (!(TCPServer.lm.Lock(id, CUSTOMER+customerId, LockManager.WRITE) && TCPServer.lm.Lock(id, CAR+location, LockManager.WRITE))) {
                return false;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }
            if(myMWRunnable.reserveCar(id, customerId, location)) {
                undoStack.add("unreserveItem" + "," + id + "," + customerId + "," + Car.getKey(location) + "," + location);
                return true;
            } else return false;
        } catch (DeadlockException e) {
            abort();
            return false;
        }
    }

    @Override
    public boolean reserveRoom(int id, int customerId, String location) {
        try {
            renewTTLCountDown();
            if (!(TCPServer.lm.Lock(id, CUSTOMER+customerId, LockManager.WRITE) && TCPServer.lm.Lock(id, ROOM+location, LockManager.WRITE))) {
                return false;
            }
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }
            if(myMWRunnable.reserveRoom(id, customerId, location)) {
                undoStack.add("unreserveItem" + "," + id + "," + customerId + "," + Room.getKey(location) + "," + location);
                return true;
            } else return false;
        } catch (DeadlockException e) {
            abort();
            return false;
        }
    }

    @Override
    public boolean reserveItinerary(int id, int customerId, Vector flightNumbers, String location, boolean car, boolean room) {
        try {
            renewTTLCountDown();
            if (!(
                    TCPServer.lm.Lock(id, CUSTOMER+customerId, LockManager.WRITE) &&
                            TCPServer.lm.Lock(id, CAR+location, LockManager.WRITE) &&
                            TCPServer.lm.Lock(id, ROOM+location, LockManager.WRITE))) {
                return false;
            }
            Iterator it = flightNumbers.iterator();
            boolean flightSuccess = true;
            while (it.hasNext()) {
                try {
                    Object oFlightNumber = it.next();
                    if (!TCPServer.lm.Lock(id, FLIGHT+myMWRunnable.getInt(oFlightNumber), LockManager.WRITE)) flightSuccess = false ;

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (!flightSuccess) return false;

            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }


            int undoCmdCount = 0;
            boolean allSuccessfulReservation = true;

            Iterator it2 = flightNumbers.iterator();
            while (it.hasNext()) {
                try {
                    Object oFlightNumber = it2.next();
                    if (myMWRunnable.reserveFlight(id, customerId, myMWRunnable.getInt(oFlightNumber))) {
                        undoStack.add("unreserveItem" + "," + id + "," + customerId + "," + Flight.getKey(myMWRunnable.getInt(oFlightNumber)) + "," + myMWRunnable.getInt(oFlightNumber));
                        undoCmdCount++;
                    } else {
                        allSuccessfulReservation = false;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (car) {
                if (myMWRunnable.reserveCar(id, customerId, location)) {
                    undoStack.add("unreserveItem" + "," + id + "," + customerId + "," + Car.getKey(location) + "," + location);
                    undoCmdCount++;
                } else allSuccessfulReservation = false;
            }
            if (room) {
                if (myMWRunnable.reserveRoom(id, customerId, location)) {
                    undoStack.add("unreserveItem" + "," + id + "," + customerId + "," + Room.getKey(location) + "," + location);
                    undoCmdCount++;
                } else allSuccessfulReservation = false;
            }

            if (allSuccessfulReservation) {
                return true;
            }
            else {
                int stopIndex = undoStack.size() - undoCmdCount;
                for (int i = undoStack.size() - 1; i >= stopIndex; i--) {
                    executeUndoLine(undoStack.get(i));
                    undoStack.remove(i);
                }
                return false;
            }

        } catch (DeadlockException e) {
            abort();
            return false;
        }
    }

    @Override
    public boolean increaseReservableItemCount(int id, String key, int count) {
        return false;
    }
}