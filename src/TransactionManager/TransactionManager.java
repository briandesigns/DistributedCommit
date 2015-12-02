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
import java.util.concurrent.*;

public class TransactionManager implements ResourceManager {
    public static Hashtable<Integer, boolean[]> transactionTable;
    public ArrayList<Customer> customers;
    private static int uniqueTransactionID = 0;
    private int currentActiveTransactionID;
    public static final int UNUSED_TRANSACTION_ID = -1;
    private MiddlewareRunnable myMWRunnable;
    private boolean inTransaction = false;
    private Thread TTLCountDownThread;
    private static final int TTL_MS = 60000;
    private static final int RM_RESPONSE_MS = 5000;
    public static final String CAR = "car-";
    public static final String FLIGHT = "flight-";
    public static final String ROOM = "room-";
    public static final String CUSTOMER = "customer-";
    public RMHashtable t_itemHT_customer;
    public RMHashtable t_itemHT_flight;
    public RMHashtable t_itemHT_car;
    public RMHashtable t_itemHT_room;


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

    private Thread startRMResponseCountDown(final String RMType) {
        Thread countDown = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().sleep(RM_RESPONSE_MS);
                } catch (InterruptedException e) {
                    Trace.info("RM responded before time out");
                }
            }
        });
        countDown.start();
        return countDown;
    }


    private void stopRMResponseCountDown(Thread countDownThread) {
        countDownThread.interrupt();
    }


    public TransactionManager(MiddlewareRunnable myMWRunnable) {
        this.myMWRunnable = myMWRunnable;
        this.customers = new ArrayList<Customer>();
        this.t_itemHT_customer = new RMHashtable();
        this.t_itemHT_flight = new RMHashtable();
        this.t_itemHT_car = new RMHashtable();
        this.t_itemHT_room = new RMHashtable();
    }

    public static synchronized boolean noActiveTransactions() {
        return transactionTable.isEmpty();
    }

    private static synchronized int generateUniqueXID() {
        uniqueTransactionID++;
        return uniqueTransactionID;
    }

    // Basic operations on ResourceManager.RMItem //

    // Read a data item.
    private RMItem readData(int id, String key) {
        if (key.contains(CUSTOMER)) {
            return (RMItem) t_itemHT_customer.get(key);
        } else if (key.contains(FLIGHT)) {
            return (RMItem) t_itemHT_flight.get(key);
        } else if (key.contains(CAR)) {
            return (RMItem) t_itemHT_car.get(key);
        } else if (key.contains(ROOM)) {
            return (RMItem) t_itemHT_room.get(key);
        } else return null;
    }

    // Write a data item.
    private void writeData(int id, String key, RMItem value) {
        if (key.contains(CUSTOMER)) {
            t_itemHT_customer.put(key, value);
        } else if (key.contains(FLIGHT)) {
            t_itemHT_flight.put(key, value);
        } else if (key.contains(CAR)) {
            t_itemHT_car.put(key, value);
        } else if (key.contains(ROOM)) {
            t_itemHT_room.put(key, value);
        }
    }

    private void addLocalItem(int id, String key,
                              int numItem, int price, int numReserved, Customer customer) {
        if (key.contains(CUSTOMER)) {
            writeData(id, key, customer);
        } else if (key.contains(FLIGHT)) {
            Flight newObj = new Flight(Integer.parseInt(key.replace(FLIGHT, "")), numItem, price);
            newObj.setReserved(numReserved);
            writeData(id, key, newObj);
        } else if (key.contains(CAR)) {
            Car newObj = new Car(key.replace(CAR, ""), numItem, price);
            newObj.setReserved(numReserved);
            writeData(id, key, newObj);
        } else if (key.contains(ROOM)) {
            Room newObj = new Room(key.replace(CAR, ""), numItem, price);
            newObj.setReserved(numReserved);
            writeData(id, key, newObj);
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
        String masterRecord = TCPServer.diskOperator.readMasterRecord();
        if (masterRecord.contains("A")) {
            try {
                TCPServer.m_itemHT_customer = (RMHashtable) TCPServer.diskOperator.getDataFromDisk("customerA");
            } catch (IOException e) {
                e.printStackTrace();
                Trace.error("failed to recover previous state from stable storage, deleting all data");
                TCPServer.m_itemHT_customer.clear();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            myMWRunnable.toFlight.println("abortA");
            try {
                Trace.info("FLIGHT RM response for abortA:" + myMWRunnable.fromFlight.readLine());
            } catch (IOException e) {
                e.printStackTrace();
            }

            myMWRunnable.toCar.println("abortA");
            try {
                Trace.info("CAR RM response for abortA:" + myMWRunnable.fromCar.readLine());
            } catch (IOException e) {
                e.printStackTrace();
            }
            myMWRunnable.toRoom.println("abortA");
            try {
                Trace.info("ROOM RM response for abortA:" + myMWRunnable.fromRoom.readLine());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (masterRecord.contains("B")) {
            try {
                TCPServer.m_itemHT_customer = (RMHashtable) TCPServer.diskOperator.getDataFromDisk("customerB");
            } catch (IOException e) {
                e.printStackTrace();
                Trace.error("failed to recover previous state from stable storage, deleting all data");
                TCPServer.m_itemHT_customer.clear();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            myMWRunnable.toFlight.println("abortB");
            try {
                Trace.info("FLIGHT RM response for abortB:" + myMWRunnable.fromFlight.readLine());
            } catch (IOException e) {
                e.printStackTrace();
            }
            myMWRunnable.toCar.println("abortB");
            try {
                Trace.info("CAR RM response for abortA:" + myMWRunnable.fromCar.readLine());
            } catch (IOException e) {
                e.printStackTrace();
            }
            myMWRunnable.toRoom.println("abortB");
            try {
                Trace.info("ROOM RM response for abortB:" + myMWRunnable.fromRoom.readLine());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        t_itemHT_room.clear();
        t_itemHT_car.clear();
        t_itemHT_flight.clear();
        t_itemHT_customer.clear();
        setInTransaction(false);
        System.out.println("txn: " + this.currentActiveTransactionID + " abort successful");
        transactionTable.remove(this.currentActiveTransactionID);
        TCPServer.lm.UnlockAll(currentActiveTransactionID);
        currentActiveTransactionID = UNUSED_TRANSACTION_ID;
        return true;
    }

    private boolean mergeCustomers(String shadowVersion) {
        Set<String> keys = t_itemHT_customer.keySet();
        for (String key : keys) {

            if (((Customer) (t_itemHT_customer.get(key))).gotDeleted()) {
                myMWRunnable.removeData(getCurrentActiveTransactionID(), key);
            } else {
                myMWRunnable.writeData(getCurrentActiveTransactionID(), key, (RMItem) t_itemHT_customer.get(key));
            }
        }
        try {
            TCPServer.diskOperator.writeDataToDisk(TCPServer.m_itemHT_customer, "customer" + shadowVersion);
        } catch (IOException e) {
            Trace.error("failed to write main memory to disk for MW");
            return false;
        }
        return true;
    }

    private boolean mergeFlights(final String shadowVersion) {
        boolean success = true;
        Set<String> keys = t_itemHT_flight.keySet();
        for (String key : keys) {
            Flight localFlight = (Flight) t_itemHT_flight.get(key);
            myMWRunnable.toFlight.println("writecompletedata" + "," + getCurrentActiveTransactionID() + "," + key
                    + "," + localFlight.getCount() + "," + localFlight.getPrice() + "," + localFlight.getReserved());
            try {
                String response = myMWRunnable.fromFlight.readLine();
                if (response == null) {
                    Trace.error("cannot get response, FLIGHT RM timed out during while voting");
                    return false;
                }
                if (response.contains("false")) {
                    Trace.error("response received from FLIGHT RM, but it failed to merge change to memory");
                    success = false;
                }

            } catch (IOException e) {
                e.printStackTrace();
                Trace.error("failed to receive response from FLIGHT RM");
                return false;
            }
        }
        if (success) {
            myMWRunnable.toFlight.println("write" + shadowVersion);
            try {
                String response = myMWRunnable.fromFlight.readLine();
                if (response == null) {
                    Trace.error("cannot get response, FLIGHT RM timed out during disk write phase of prepare");
                    return false;
                }
                if (response.contains("false")) {
                    Trace.error("failed to write memory to disk for Flight RM");
                    success = false;
                }
            } catch (IOException e) {
                Trace.error("failed to write main memory to disk for Flight RM");
                success = false;
            }
        }
        return success;
    }

    private boolean mergeCars(final String shadowVersion) {
        boolean success = true;
        Set<String> keys = t_itemHT_car.keySet();
        for (String key : keys) {
            Car localCar = (Car) t_itemHT_car.get(key);
            myMWRunnable.toCar.println("writecompletedata" + "," + getCurrentActiveTransactionID() + "," + key + ","
                    + localCar.getCount() + "," + localCar.getPrice() + "," + localCar.getReserved());
            try {
                String response = myMWRunnable.fromCar.readLine();
                if (response == null) {
                    Trace.error("cannot get response, CAR RM timed out during while voting");
                    return false;
                }
                if (response.contains("false")) {
                    Trace.error("response received from CAR RM, but it failed to merge change to memory");
                    success = false;
                }
            } catch (IOException e) {
                Trace.error("failed to receive response from CAR RM");
                return false;
            }
        }
        if (success) {
            myMWRunnable.toCar.println("write" + shadowVersion);
            try {
                String response = myMWRunnable.fromCar.readLine();
                if (response == null) {
                    Trace.error("cannot get response, CAR RM timed out during disk write phase of prepare");
                    return false;
                }
                if (response.contains("false")) {
                    Trace.error("failed to write memory to disk for Flight RM");
                    success = false;
                }
            } catch (IOException e) {
                Trace.error("failed to write memory to disk for Car RM");
                success = false;
            }
        }
        return success;
    }

    private boolean mergeRooms(final String shadowVersion) {
        boolean success = true;
        Set<String> keys = t_itemHT_room.keySet();
        for (String key : keys) {
            Room localRoom = (Room) t_itemHT_room.get(key);
            myMWRunnable.toRoom.println("writecompletedata" + "," + getCurrentActiveTransactionID() + "," + key + ","
                    + localRoom.getCount() + "," + localRoom.getPrice() + "," + localRoom.getReserved());
            try {
                String response = myMWRunnable.fromRoom.readLine();
                if (response == null) {
                    Trace.error("cannot get response, ROOM RM timed out during while voting");
                    return false;
                }
                if (response.contains("false")) {
                    Trace.error("response received from ROOM RM, but it failed to merge change to memory");
                    success = false;
                }
            } catch (IOException e) {
                e.printStackTrace();
                Trace.error("failed to receive response from ROOM RM");
                return false;
            }
        }
        if (success) {
            myMWRunnable.toRoom.println("write" + shadowVersion);
            try {
                String response = myMWRunnable.fromRoom.readLine();
                if(response == null) {
                    Trace.error("cannot get response, CAR RM timed out during disk write phase of prepare");
                    return false;
                }
                if (response.contains("false")) {
                    Trace.error("failed to write memory to disk for Room RM");
                    success = false;
                }
            } catch (IOException e) {
                e.printStackTrace();
                success = false;
            }
        }
        return success;
    }


    private boolean prepare(final String shadowVersion) {
        final Boolean[] flightReady = {null};
        final Boolean[] carReady = {null};
        final Boolean[] roomReady = {null};
        final Boolean[] customerReady = {null};
        new Thread(new Runnable() {
            @Override
            public void run() {
                flightReady[0] = mergeFlights(shadowVersion);
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                carReady[0] = mergeCars(shadowVersion);
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                roomReady[0] = mergeRooms(shadowVersion);
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                customerReady[0] = mergeCustomers(shadowVersion);
            }
        }).start();
        while (flightReady[0] == null || carReady[0] == null || roomReady[0] == null || customerReady[0] == null) {
            try {
                Thread.currentThread().sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        boolean success = true;
        if (!flightReady[0] || !carReady[0] || !roomReady[0] || !customerReady[0]) success = false;
        return success;
    }

    private String getShadowVersion() {
        String shadowVersion;
        if (TCPServer.diskOperator.readMasterRecord().contains("A")) {
            shadowVersion = "B";
        } else if (TCPServer.diskOperator.readMasterRecord().contains("B")) {
            shadowVersion = "A";
        } else {
            shadowVersion = "A";
        }
        return shadowVersion;
    }

    public boolean commit() {
        try {
            if (!TCPServer.lm.Lock(getCurrentActiveTransactionID(), "GLOBAL_WRITE", LockManager.WRITE)) {
                return false;
            }
            stopTTLCountDown();
            String shadowVersion = getShadowVersion();
            TCPServer.diskOperator.clearLogRecord();
            TCPServer.diskOperator.writeLogRecord("START,");

            if (!prepare(shadowVersion)) {
                Trace.error("failed to merge local copy with main memory copy and creating before/after image. " +
                        "aborting");
                TCPServer.diskOperator.writeLogRecord("ABORT");
                abort();
                return false;
            }
            TCPServer.diskOperator.writeLogRecord("COMMIT"+shadowVersion);
            boolean success2 = TCPServer.diskOperator.writeMasterRecord(shadowVersion + getCurrentActiveTransactionID());
            if (!success2) {
                Trace.error("could not write master record. aborting");
                TCPServer.diskOperator.clearLogRecord();
                TCPServer.diskOperator.writeLogRecord("START,");
                TCPServer.diskOperator.writeLogRecord("ABORT");
                abort();
            }
//
//            myMWRunnable.toFlight.println("commit" + shadowVersion);
//            try {
//                myMWRunnable.fromFlight.readLine();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            myMWRunnable.toCar.println("commit" + shadowVersion);
//            try {
//                myMWRunnable.fromCar.readLine();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            myMWRunnable.toRoom.println("commit" + shadowVersion);
//            try {
//                myMWRunnable.fromRoom.readLine();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

            Trace.info("commit saved to shadow" + shadowVersion);
            t_itemHT_room.clear();
            t_itemHT_car.clear();
            t_itemHT_flight.clear();
            t_itemHT_customer.clear();
            TCPServer.lm.UnlockAll(this.currentActiveTransactionID);
            setInTransaction(false);
            transactionTable.remove(this.currentActiveTransactionID);
            Trace.info("successfully performed commit");
            currentActiveTransactionID = UNUSED_TRANSACTION_ID;
            return success2;
        } catch (DeadlockException e) {
            e.printStackTrace();
            Trace.error("Deadlock on commit");
            abort();
            return false;
        }
    }


    @Override
    public boolean addFlight(int id, int flightNumber, int numSeats, int flightPrice) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return false;
            }
            if (!TCPServer.lm.Lock(id, FLIGHT + flightNumber, LockManager.WRITE)) {
                return false;
            }

            Flight localFlight = (Flight) readData(id, Flight.getKey(flightNumber));

            if (localFlight == null) {
                if (myMWRunnable.isExistingFlight(id, flightNumber)) {
                    addLocalItem(id, FLIGHT + flightNumber, myMWRunnable.queryFlight(id, flightNumber) +
                            numSeats, flightPrice, myMWRunnable.queryFlightReserved(id, flightNumber), null);
                } else {
                    addLocalItem(id, FLIGHT + flightNumber, numSeats, flightPrice, 0, null);
                }
            } else {
                if (localFlight.getCount() == -1) {
                    localFlight.setCount(numSeats);
                    localFlight.setPrice(flightPrice);
                } else
                    localFlight.setCount(localFlight.getCount() + numSeats);
                if (flightPrice > 0) localFlight.setPrice(flightPrice);
            }

            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{true, false, false, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[0] = true;
            }
            Trace.info("successfully added flight");
            return true;
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on addFlight");
            return false;
        }
    }


    @Override
    public boolean deleteFlight(int id, int flightNumber) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return false;
            }
            if (!TCPServer.lm.Lock(id, FLIGHT + flightNumber, LockManager.WRITE)) {
                return false;
            }
            Flight localFlight = (Flight) readData(id, Flight.getKey(flightNumber));
            if (localFlight == null) {
                if (myMWRunnable.isExistingFlight(id, flightNumber)) {
                    if (myMWRunnable.queryFlightReserved(id, flightNumber) == 0)
                        addLocalItem(id, FLIGHT + flightNumber, -1, -1,
                                myMWRunnable.queryFlightReserved(id, flightNumber), null);
                    else {
                        addLocalItem(id, FLIGHT + flightNumber, myMWRunnable.queryFlight(id, flightNumber),
                                myMWRunnable.queryFlightPrice(id, flightNumber),
                                myMWRunnable.queryFlightReserved(id, flightNumber), null);
                        Trace.error("cannot delete flight due to existing reservations held by Customers");
                        return false;
                    }
                } else {
                    Trace.error("cannot delete flight, flight does not exist");
                    return false;
                }
            } else {
                if (localFlight.getReserved() == 0) {
                    localFlight.setCount(-1);
                    localFlight.setPrice(-1);
                } else {
                    Trace.error("flight cannot be deleted due to existing reservations held by customers");
                    return false;
                }
            }
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{true, false, false, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[0] = true;
            }
            Trace.info("flight successfully deleted");
            return true;
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on deleteFlight");
            return false;
        }
    }

    @Override
    public int queryFlight(int id, int flightNumber) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return -3;
            }
            if (!TCPServer.lm.Lock(id, FLIGHT + flightNumber, LockManager.READ)) {
                return -2;
            }
            Flight localFlight = (Flight) readData(id, Flight.getKey(flightNumber));
            if (localFlight == null) {
                Trace.info("successfully performed queryFlight");
                return myMWRunnable.queryFlight(id, flightNumber);
            } else {
                Trace.info("successfully performed queryFlight");
                return localFlight.getCount();
            }
        } catch (DeadlockException e) {
//            e.printStackTrace();
            abort();
            Trace.error("Deadlock on queryFlight");
            return -2;
        }
    }

    @Override
    public int queryFlightPrice(int id, int flightNumber) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return -3;
            }
            if (!TCPServer.lm.Lock(id, FLIGHT + flightNumber, LockManager.READ)) {
                return -2;
            }
            Flight localFlight = (Flight) readData(id, Flight.getKey(flightNumber));
            if (localFlight == null) {
                Trace.info("successfully performed queryFlightPrice");
                return myMWRunnable.queryFlightPrice(id, flightNumber);
            } else {
                Trace.info("successfully performed queryFlightPrice");
                return localFlight.getPrice();
            }
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on queryFlightPrice");
            return -2;
        }
    }

    @Override
    public boolean addCars(int id, String location, int numCars, int carPrice) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return false;
            }
            if (!TCPServer.lm.Lock(id, CAR + location, LockManager.WRITE)) {
                return false;
            }
            Car localCar = (Car) readData(id, Car.getKey(location));
            if (localCar == null) {
                if (myMWRunnable.isExistingCars(id, location)) {
                    addLocalItem(id, CAR + location, myMWRunnable.queryCars(id, location) + numCars,
                            carPrice, myMWRunnable.queryCarsReserved(id, location), null);
                } else {
                    addLocalItem(id, CAR + location, numCars, carPrice, 0, null);
                }
            } else {
                if (localCar.getCount() == -1) {
                    localCar.setCount(numCars);
                    localCar.setPrice(carPrice);
                } else
                    localCar.setCount(localCar.getCount() + numCars);
                if (carPrice > 0) localCar.setPrice(carPrice);
            }
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, true, false, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[1] = true;
            }
            Trace.info("successfully performed addCars");
            return true;
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on addCars");
            return false;
        }
    }

    @Override
    public boolean deleteCars(int id, String location) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return false;
            }
            if (!TCPServer.lm.Lock(id, CAR + location, LockManager.WRITE)) {
                return false;
            }
            Car localCar = (Car) readData(id, Car.getKey(location));
            if (localCar == null) {
                if (myMWRunnable.isExistingCars(id, location)) {
                    if (myMWRunnable.queryCarsReserved(id, location) == 0)
                        addLocalItem(id, CAR + location, -1, -1, myMWRunnable.queryCarsReserved(id, location), null);
                    else {
                        addLocalItem(id, CAR + location, myMWRunnable.queryCars(id, location),
                                myMWRunnable.queryCarsPrice(id, location),
                                myMWRunnable.queryCarsReserved(id, location), null);
                        Trace.error("cannot delete car due to existing reservations held by Customers");
                        return false;
                    }
                } else {
                    Trace.error("cannot delete car, car does not exist");
                    return false;
                }
            } else {
                if (localCar.getReserved() == 0) {
                    localCar.setCount(-1);
                    localCar.setPrice(-1);
                } else {
                    Trace.error("cannot delete car due to existing reservations held by Customers");
                    return false;
                }
            }
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, true, false, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[1] = true;
            }
            Trace.info("successfully performed deleteCars");
            return true;
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock n deleteCars");
            return false;
        }
    }

    @Override
    public int queryCars(int id, String location) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return -3;
            }
            if (!TCPServer.lm.Lock(id, CAR + location, LockManager.READ)) {
                return -2;
            }
            Car localCar = (Car) readData(id, Car.getKey(location));
            if (localCar == null) {
                Trace.info("successfully performed queryCars");
                return myMWRunnable.queryCars(id, location);
            } else {
                Trace.info("successfully performed queryCars");
                return localCar.getCount();
            }
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on queryCars");
            return -2;
        }
    }

    @Override
    public int queryCarsPrice(int id, String location) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return -3;
            }
            if (!TCPServer.lm.Lock(id, CAR + location, LockManager.READ)) {
                return -2;
            }
            Car localCar = (Car) readData(id, Car.getKey(location));
            if (localCar == null) {
                Trace.info("successfully performed queryCarsPrice");
                return myMWRunnable.queryCarsPrice(id, location);
            } else {
                Trace.info("successfully performed queryCarsPrice");
                return localCar.getPrice();
            }
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on queryCarsPrice");
            return -2;
        }
    }

    @Override
    public boolean addRooms(int id, String location, int numRooms, int roomPrice) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return false;
            }
            if (!TCPServer.lm.Lock(id, ROOM + location, LockManager.WRITE)) {
                return false;
            }


            Room localRoom = (Room) readData(id, Room.getKey(location));
            if (localRoom == null) {
                if (myMWRunnable.isExistingRooms(id, location)) {
                    addLocalItem(id, ROOM + location, myMWRunnable.queryRooms(id, location) + numRooms,
                            roomPrice, myMWRunnable.queryRoomsReserved(id, location), null);
                } else {
                    addLocalItem(id, ROOM + location, numRooms, roomPrice, 0, null);
                }
            } else {
                if (localRoom.getCount() == -1) {
                    localRoom.setCount(numRooms);
                    localRoom.setPrice(roomPrice);
                } else
                    localRoom.setCount(localRoom.getCount() + numRooms);
                if (roomPrice > 0) localRoom.setPrice(roomPrice);
            }
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, false, true, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[2] = true;
            }
            Trace.info("Successfully performed addRooms");
            return true;
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on addRooms");
            return false;
        }
    }

    @Override
    public boolean deleteRooms(int id, String location) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return false;
            }
            if (!TCPServer.lm.Lock(id, ROOM + location, LockManager.WRITE)) {
                return false;
            }
            Room localRoom = (Room) readData(id, Room.getKey(location));
            if (localRoom == null) {
                if (myMWRunnable.isExistingRooms(id, location)) {
                    if (myMWRunnable.queryRoomsReserved(id, location) == 0)
                        addLocalItem(id, ROOM + location, -1, -1, myMWRunnable.queryRoomsReserved(id, location), null);
                    else {
                        addLocalItem(id, ROOM + location, myMWRunnable.queryRooms(id, location),
                                myMWRunnable.queryRoomsPrice(id, location),
                                myMWRunnable.queryRoomsReserved(id, location), null);
                        Trace.error("cannot delete room due to existing reservations held by Customers");
                        return false;
                    }
                } else {
                    Trace.error("cannot delete room, room does not exist");
                    return false;
                }
            } else {
                if (localRoom.getReserved() == 0) {
                    localRoom.setCount(-1);
                    localRoom.setPrice(-1);
                } else {
                    Trace.error("cannot delete room, room does not exist");
                    return false;
                }
            }
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, false, true, false};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[2] = true;
            }
            Trace.info("Successfully performed deleteRooms");
            return true;
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on deleteRooms");
            return false;
        }
    }

    @Override
    public int queryRooms(int id, String location) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return -3;
            }
            if (!TCPServer.lm.Lock(id, ROOM + location, LockManager.READ)) {
                return -2;
            }
            Room localRoom = (Room) readData(id, Room.getKey(location));
            if (localRoom == null) {
                Trace.info("successfully performed queryRooms");
                return myMWRunnable.queryRooms(id, location);
            } else {
                Trace.info("successfully performed queryRooms");
                return localRoom.getCount();
            }
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on queryRooms");
            return -2;
        }
    }

    @Override
    public int queryRoomsPrice(int id, String location) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return -3;
            }
            if (!TCPServer.lm.Lock(id, ROOM + location, LockManager.READ)) {
                return -2;
            }
            Room localRoom = (Room) readData(id, Room.getKey(location));
            if (localRoom == null) {
                Trace.info("successfully performed queryRoomsPrice");
                return myMWRunnable.queryRoomsPrice(id, location);
            } else {
                Trace.info("successfully performed queryRoomsPrice");
                return localRoom.getPrice();
            }
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on queryRoomsPrice");
            return -2;
        }
    }

    @Override
    public int newCustomer(int id) {

        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return -3;
            }
            // Generate a globally unique Id for the new customer.
            int customerId = Integer.parseInt(String.valueOf(id) +
                    String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND)) +
                    String.valueOf(Math.round(Math.random() * 100 + 1)));
            if (!TCPServer.lm.Lock(id, CUSTOMER + customerId, LockManager.WRITE)) {
                return -2;
            }

            Customer localCustomer = (Customer) readData(id, CUSTOMER + customerId);

            if (localCustomer != null) {
                if (localCustomer.getReservations() != null) {
                    Trace.error("customer already exist, cannot add it");
                    return -1;
                } else {
                    localCustomer = new Customer(customerId);
                    addLocalItem(id, CUSTOMER + customerId, 0, 0, 0, localCustomer);
                }
            } else {
                localCustomer = (Customer) myMWRunnable.readData(id, CUSTOMER + customerId);
                if (localCustomer == null) {
                    localCustomer = new Customer(customerId);
                    addLocalItem(id, CUSTOMER + customerId, 0, 0, 0, localCustomer);
                } else {
                    Trace.error("customer already exist, cannot add it");
                    return -1;
                }

            }

            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, false, false, true};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[3] = true;
            }
            Trace.info("successfully performed newCustomer");
            return customerId;
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on newCustomer");
            return -2;
        }
    }

    @Override
    public boolean newCustomerId(int id, int customerId) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return false;
            }
            if (!(TCPServer.lm.Lock(id, CUSTOMER + customerId, LockManager.WRITE))) {
                return false;
            }
            Customer localCustomer = (Customer) readData(id, CUSTOMER + customerId);

            if (localCustomer != null) {
                if (localCustomer.getReservations() != null) {
                    Trace.error("customer already exist, cannot add it");
                    return false;
                } else {
                    localCustomer = new Customer(customerId);
                    addLocalItem(id, CUSTOMER + customerId, 0, 0, 0, localCustomer);
                }
            } else {
                localCustomer = (Customer) myMWRunnable.readData(id, CUSTOMER + customerId);
                if (localCustomer == null) {
                    localCustomer = new Customer(customerId);
                    addLocalItem(id, CUSTOMER + customerId, 0, 0, 0, localCustomer);
                } else {
                    Trace.error("customer already exist, cannot add it");
                    return false;
                }

            }
            boolean[] involvedRMs = transactionTable.get(id);
            if (involvedRMs == null) {
                involvedRMs = new boolean[]{false, false, false, true};
                transactionTable.put(id, involvedRMs);
            } else {
                involvedRMs[3] = true;
            }
            Trace.info("successfully performed newCustomerId");
            return true;
        } catch (DeadlockException e) {
//            e.printStackTrace();
            abort();
            Trace.error("Deadlock on newCustomerId");
            return false;
        }
    }

    @Override
    public boolean deleteCustomer(int id, int customerId) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return false;
            }
            if (!(TCPServer.lm.Lock(id, CUSTOMER + customerId, LockManager.WRITE))) {
                return false;
            }

            Customer localCustomer = (Customer) readData(id, Customer.getKey(customerId));
            if (localCustomer == null) {
                Customer memoryCustomer = (Customer) myMWRunnable.readData(id, Customer.getKey(customerId));
                if (memoryCustomer == null) {
                    Trace.error("customer does not exist, failed to perform deleteCustomer");
                    return false;
                } else {
                    localCustomer = memoryCustomer.clone();
                    addLocalItem(id, localCustomer.getKey(), 0, 0, 0, localCustomer);

                }
            }

            if (localCustomer.gotDeleted()) {
                Trace.error("customer does not exist. Cannot delete it");
                return false;
            }

            if (!getLockOnReservedItems(id, localCustomer)) {
                Trace.error("could not get lock on customer reservedItems");
                return false;
            }
            if (!updateReservations(id, localCustomer)) {
                Trace.error("failed to update reservations. Failed to deleteCustomer");
                return false;
            }
            localCustomer.clearReservations();
            Trace.info("successfully updated reservations and performed deleteCustomer");
            return true;
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on deleteCustomer");
            return false;
        }
    }

    public boolean getLockOnReservedItems(int id, Customer localCustomer) {
        boolean success = true;
        RMHashtable reservationHT = localCustomer.getReservations();
        for (Enumeration e = reservationHT.keys(); e.hasMoreElements(); ) {
            String reservedKey = (String) (e.nextElement());
            ReservedItem reservedItem = localCustomer.getReservedItem(reservedKey);
            try {
                if (reservedItem.getKey().contains(FLIGHT)) {
                    if (!TCPServer.lm.Lock(id, reservedItem.getKey(), LockManager.WRITE)) success = false;
                } else if (reservedItem.getKey().contains(CAR)) {
                    if (!TCPServer.lm.Lock(id, reservedItem.getKey(), LockManager.WRITE)) success = false;
                } else if (reservedItem.getKey().contains(ROOM)) {
                    if (!TCPServer.lm.Lock(id, reservedItem.getKey(), LockManager.WRITE)) success = false;
                } else {
                    Trace.error("reserved item does not exist");
                    return false;
                }
                return success;
            } catch (DeadlockException e1) {
                abort();
                e1.printStackTrace();
                Trace.error("Deadlock on getLockOnReservedItems");
                return false;
            }
        }
        return success;
    }

    private boolean updateReservations(int id, Customer localCustomer) {
        boolean reservableItemUpdated = true;
        // Increase the reserved numbers of all reservable items that
        // the customer reserved.
        RMHashtable reservationHT = localCustomer.getReservations();
        for (Enumeration e = reservationHT.keys(); e.hasMoreElements(); ) {
            String reservedKey = (String) (e.nextElement());
            ReservedItem reservedItem = localCustomer.getReservedItem(reservedKey);
            if (!increaseReservableItemCount(id, reservedItem.getKey(), reservedItem.getCount()))
                reservableItemUpdated = false;
        }
        return reservableItemUpdated;
    }

    @Override
    public String queryCustomerInfo(int id, int customerId) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return "transaction ID does not match current transaction, command ignored";
            }
            if (!(TCPServer.lm.Lock(id, CUSTOMER + customerId, LockManager.READ))) {
                return "can't get customer Info since can't get lock";
            }
            Customer localCustomer = (Customer) readData(id, Customer.getKey(customerId));
            if (localCustomer == null) {
                return myMWRunnable.queryCustomerInfo(id, customerId);
            } else {
                if (localCustomer.gotDeleted()) {
                    return "customer does not exist";
                } else {
                    return localCustomer.printBill();
                }
            }
        } catch (DeadlockException e) {
            e.printStackTrace();
            abort();
            Trace.error("Deadlock on queryCustomerInfo");
            return "can't get customer Info";
        }
    }


    protected boolean reserveItem(int id, int customerId,
                                  String key, String location) throws Exception {
        Trace.info("RM::reserveItem(" + id + ", " + customerId + ", "
                + key + ", " + location + ") called.");
        // Read customer object if it exists (and read lock it).
        Customer localCustomer = (Customer) readData(id, Customer.getKey(customerId));
        if (localCustomer == null) {
            Customer memoryCustomer = (Customer) myMWRunnable.readData(id, Customer.getKey(customerId));
            if (memoryCustomer == null) {
                Trace.error("customer does not exist, failed to perform reserveItem");
                return false;
            } else {
                localCustomer = memoryCustomer.clone();
                addLocalItem(id, localCustomer.getKey(), 0, 0, 0, localCustomer);
            }
        }

        if (localCustomer.getReservations() == null) {
            Trace.error("customer does not exist, failed to perform reserveItem");
            return false;
        }

        //Check for item availability and getting price
        boolean isSuccessfulReservation = false;
        int itemPrice = -1;
        isSuccessfulReservation = decreaseReservableItemCount(id, key, 1);
        if (isSuccessfulReservation) {
            if (key.contains(FLIGHT)) {
                itemPrice = queryFlightPrice(id, Integer.parseInt(key.replace(FLIGHT, "")));
            } else if (key.contains(CAR)) {
                itemPrice = queryCarsPrice(id, key.replace(CAR, ""));
            } else if (key.contains(ROOM)) {
                itemPrice = queryRoomsPrice(id, key.replace(ROOM, ""));
            }
        } else {
            Trace.error("could not make reservation due to decrease count problems");
            return false;
        }

        if (itemPrice == -1) {
            Trace.error("could not get item price, price set to 0");
            itemPrice = 0;
        }

        // Do reservation.

        localCustomer.reserve(key, location, itemPrice);
        //this should be redundant code
        writeData(id, localCustomer.getKey(), localCustomer);

        Trace.warn("RM::reserveItem(" + id + ", " + customerId + ", "
                + key + ", " + location + ") OK.");
        return true;

    }

    @Override
    public boolean reserveFlight(int id, int customerId, int flightNumber) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return false;
            }
            if (!(TCPServer.lm.Lock(id, CUSTOMER + customerId, LockManager.WRITE)
                    && TCPServer.lm.Lock(id, FLIGHT + flightNumber, LockManager.WRITE))) {
                return false;
            }

            boolean success = reserveItem(id, customerId, Flight.getKey(flightNumber), String.valueOf(flightNumber));
            if (success) {
                Trace.info("successfully performed reserveFlight");
                return true;
            } else {
                Trace.error("failed to perform reserveFlight");
                return false;
            }
        } catch (DeadlockException e) {
            abort();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            Trace.error("could not reserve flight due to error");
            return false;
        }
    }


    @Override
    public boolean reserveCar(int id, int customerId, String location) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }
            if (!(TCPServer.lm.Lock(id, CUSTOMER + customerId, LockManager.WRITE)
                    && TCPServer.lm.Lock(id, CAR + location, LockManager.WRITE))) {
                return false;
            }
            boolean success = reserveItem(id, customerId, Car.getKey(location), location);

            return success;
        } catch (DeadlockException e) {
            abort();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            Trace.error("could not reserve car due to error");
            return false;
        }
    }

    @Override
    public boolean reserveRoom(int id, int customerId, String location) {
        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                System.out.println("transaction id does not match current transaction, command ignored");
                TCPServer.lm.UnlockAll(id);

                return false;
            }
            if (!(TCPServer.lm.Lock(id, CUSTOMER + customerId, LockManager.WRITE)
                    && TCPServer.lm.Lock(id, ROOM + location, LockManager.WRITE))) {
                return false;
            }
            boolean success = reserveItem(id, customerId, Room.getKey(location), location);

            return success;
        } catch (DeadlockException e) {
            abort();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            Trace.error("could not reserve room due to error");
            return false;
        }
    }

    @Override
    public boolean reserveItinerary(int id, int customerId, Vector flightNumbers, String location, boolean car,
                                    boolean room) {

        try {
            renewTTLCountDown();
            if (this.currentActiveTransactionID != id) {
                Trace.error("transaction id does not match current transaction, command ignored");
                return false;
            }
            if (!(
                    TCPServer.lm.Lock(id, CUSTOMER + customerId, LockManager.WRITE) &&
                            TCPServer.lm.Lock(id, CAR + location, LockManager.WRITE) &&
                            TCPServer.lm.Lock(id, ROOM + location, LockManager.WRITE))) {
                return false;
            }
            Iterator it = flightNumbers.iterator();
            boolean flightSuccess = true;
            while (it.hasNext()) {
                try {
                    Object oFlightNumber = it.next();
                    if (!TCPServer.lm.Lock(id, FLIGHT + myMWRunnable.getInt(oFlightNumber), LockManager.WRITE)) {
                        flightSuccess = false;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (!flightSuccess) return false;

            Customer localCustomer = (Customer) readData(id, Customer.getKey(customerId));
            if (localCustomer == null) {
                Customer memoryCustomer = (Customer) myMWRunnable.readData(id, Customer.getKey(customerId));
                if (memoryCustomer == null) {
                    Trace.error("customer does not exist, failed to perform deleteCustomer");
                    return false;
                } else {
                    localCustomer = memoryCustomer.clone();
                    addLocalItem(id, localCustomer.getKey(), 0, 0, 0, localCustomer);

                }
            }
            if (localCustomer.gotDeleted()) {
                Trace.error("customer does not exist, cannot performReserveitinerary");
                return false;
            }

            Customer backupCustomer = localCustomer.clone();
            ArrayList<Flight> backupFlights = new ArrayList<Flight>();
            Car backupCar = null;
            Room backupRoom = null;
            boolean allSuccessfulReservation = true;

            Iterator it2 = flightNumbers.iterator();


            while (it2.hasNext()) {
                try {
                    Object oFlightNumber = it2.next();

                    Flight localFlight = (Flight) readData(id, FLIGHT + myMWRunnable.getInt(oFlightNumber));

                    if (localFlight == null) {
                        if (myMWRunnable.isExistingFlight(id, myMWRunnable.getInt(oFlightNumber))) {
                            addLocalItem(id, FLIGHT + myMWRunnable.getInt(oFlightNumber),
                                    myMWRunnable.queryFlight(id, myMWRunnable.getInt(oFlightNumber)),
                                    myMWRunnable.queryFlightPrice(id, myMWRunnable.getInt(oFlightNumber)),
                                    myMWRunnable.queryFlightReserved(id, myMWRunnable.getInt(oFlightNumber)), null);
                            localFlight = (Flight) readData(id, FLIGHT + myMWRunnable.getInt(oFlightNumber));
                            Flight backupFlight = new Flight(myMWRunnable.getInt(oFlightNumber),
                                    localFlight.getCount(), localFlight.getPrice());
                            backupFlight.setReserved(localFlight.getReserved());
                            backupFlights.add(backupFlight);
                            if (!reserveFlight(id, customerId, myMWRunnable.getInt(oFlightNumber))) {
                                Trace.error("failed to reserve flight");
                                allSuccessfulReservation = false;
                            }
                        } else {
                            allSuccessfulReservation = false;
                        }
                    } else {
                        if (localFlight.getCount() == -1) {
                            allSuccessfulReservation = false;
                        } else {
                            Flight backupFlight = new Flight(myMWRunnable.getInt(oFlightNumber),
                                    localFlight.getCount(), localFlight.getPrice());
                            backupFlight.setReserved(localFlight.getReserved());
                            backupFlights.add(backupFlight);
                            if (!reserveFlight(id, customerId, myMWRunnable.getInt(oFlightNumber))) {
                                Trace.error("failed to reserve flight");
                                allSuccessfulReservation = false;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (car) {
                Car localCar = (Car) readData(id, Car.getKey(location));
                if (localCar == null) {
                    if (myMWRunnable.isExistingCars(id, location)) {
                        addLocalItem(id, CAR + location, myMWRunnable.queryCars(id, location),
                                queryCarsPrice(id, location), myMWRunnable.queryCarsReserved(id, location), null);
                        localCar = (Car) readData(id, Car.getKey(location));
                        backupCar = new Car(location, localCar.getCount(), localCar.getPrice());
                        backupCar.setReserved(localCar.getReserved());
                        if (!reserveCar(id, customerId, location)) {
                            allSuccessfulReservation = false;
                        }
                    } else {
                        allSuccessfulReservation = false;
                    }
                } else {
                    if (localCar.getCount() == -1) {
                        allSuccessfulReservation = false;
                    } else {
                        backupCar = new Car(location, localCar.getCount(), localCar.getPrice());
                        backupCar.setReserved(localCar.getReserved());
                        if (!reserveCar(id, customerId, location)) {
                            allSuccessfulReservation = false;
                        }
                    }

                }
            }
            if (room) {
                Room localRoom = (Room) readData(id, Room.getKey(location));
                if (localRoom == null) {
                    if (myMWRunnable.isExistingRooms(id, location)) {
                        addLocalItem(id, ROOM + location, myMWRunnable.queryRooms(id, location),
                                queryRoomsPrice(id, location), myMWRunnable.queryRoomsReserved(id, location), null);
                        localRoom = (Room) readData(id, Room.getKey(location));
                        backupRoom = new Room(location, localRoom.getCount(), localRoom.getPrice());
                        backupRoom.setReserved(localRoom.getReserved());
                        if (!reserveRoom(id, customerId, location)) {
                            allSuccessfulReservation = false;
                        }
                    } else {
                        allSuccessfulReservation = false;
                    }
                } else {
                    if (localRoom.getCount() == -1) {
                        allSuccessfulReservation = false;
                    } else {
                        backupRoom = new Room(location, localRoom.getCount(), localRoom.getPrice());
                        backupRoom.setReserved(localRoom.getReserved());
                        if (!reserveRoom(id, customerId, location)) {
                            allSuccessfulReservation = false;
                        }
                    }

                }
            }

            if (allSuccessfulReservation) {
                Trace.info("successfully performed reserveItinerary");
                return true;
            } else {
                for (int i = 0; i < backupFlights.size(); i++) {
                    t_itemHT_flight.put(backupFlights.get(i).getKey(), backupFlights.get(i));
                }
                if (backupCar != null) {
                    t_itemHT_car.put(backupCar.getKey(), backupCar);
                }
                if (backupRoom != null) {
                    t_itemHT_room.put(backupRoom.getKey(), backupRoom);
                }
                t_itemHT_customer.put(backupCustomer.getKey(), backupCustomer);
                Trace.error("One of the items in itinerary could not be reserved, itinerary cancelled");
                return false;
            }

        } catch (DeadlockException e) {
            abort();
            Trace.error("Deadlock on reserveItinerary");
            return false;
        }
    }

    public boolean increaseReservableItemCount(int id, String key, int count) {
        ReservableItem item = (ReservableItem) readData(id, key);
        if (item == null) {
            if (key.contains(FLIGHT)) {
                if (myMWRunnable.isExistingFlight(id, Integer.parseInt(key.replace(FLIGHT, "")))) {
                    addLocalItem(id, key, myMWRunnable.queryFlight(id, Integer.parseInt(key.replace(FLIGHT, ""))),
                            myMWRunnable.queryFlightPrice(id, Integer.parseInt(key.replace(FLIGHT, ""))),
                            myMWRunnable.queryFlightReserved(id, Integer.parseInt(key.replace(FLIGHT, ""))), null);
                    Flight localFlight = (Flight) readData(id, key);
                    localFlight.setReserved(localFlight.getReserved() - count);
                    localFlight.setCount(localFlight.getCount() + count);
                    return true;
                } else {
                    Trace.error("flight item does not exist, cannot increase item Count");
                    return false;
                }
            } else if (key.contains(CAR)) {
                if (myMWRunnable.isExistingCars(id, key.replace(CAR, ""))) {
                    addLocalItem(id, key, myMWRunnable.queryCars(id, key.replace(CAR, "")),
                            myMWRunnable.queryCarsPrice(id, key.replace(CAR, "")),
                            myMWRunnable.queryCarsReserved(id, key.replace(CAR, "")), null);
                    Car localCar = (Car) readData(id, key);
                    localCar.setReserved(localCar.getReserved() - count);
                    localCar.setCount(localCar.getCount() + count);
                    return true;
                } else {
                    Trace.error("car item does not exist, cannot increase item Count");
                    return false;
                }
            } else if (key.contains(ROOM)) {
                if (myMWRunnable.isExistingRooms(id, key.replace(ROOM, ""))) {
                    addLocalItem(id, key, myMWRunnable.queryRooms(id, key.replace(ROOM, "")),
                            myMWRunnable.queryRoomsPrice(id, key.replace(ROOM, "")),
                            myMWRunnable.queryRoomsReserved(id, key.replace(ROOM, "")), null);
                    Room localRoom = (Room) readData(id, key);
                    localRoom.setReserved(localRoom.getReserved() - count);
                    localRoom.setCount(localRoom.getCount() + count);
                    return true;
                } else {
                    Trace.info("room item does not exist, cannot increase item Count");
                    return false;
                }
            }
        } else {
            if (item.getCount() == -1) {
                Trace.error("item does not exist, failed to increaseReservableItemCount");
                return false;
            }
            item.setReserved(item.getReserved() - count);
            item.setCount(item.getCount() + count);
            Trace.info("item reserved: " + item.getReserved() + "    item count: " + item.getCount());
            return true;
        }
        return true;
    }

    public boolean decreaseReservableItemCount(int id, String key, int count) {
        ReservableItem item = (ReservableItem) readData(id, key);
        if (item == null) {
            if (key.contains(FLIGHT)) {
                if (myMWRunnable.isExistingFlight(id, Integer.parseInt(key.replace(FLIGHT, "")))) {
                    addLocalItem(id, key, myMWRunnable.queryFlight(id, Integer.parseInt(key.replace(FLIGHT, ""))),
                            myMWRunnable.queryFlightPrice(id, Integer.parseInt(key.replace(FLIGHT, ""))),
                            myMWRunnable.queryFlightReserved(id, Integer.parseInt(key.replace(FLIGHT, ""))), null);
                    Flight localFlight = (Flight) readData(id, key);
                    if (localFlight.getCount() >= count) {
                        localFlight.setReserved(localFlight.getReserved() + count);
                        localFlight.setCount(localFlight.getCount() - count);
                        return true;
                    } else {
                        Trace.error("Flights fully booked, cannot make reservation");
                        return false;
                    }
                } else {
                    Trace.error("flight item does not exist, cannot decrease item Count");
                    return false;
                }
            } else if (key.contains(CAR)) {
                if (myMWRunnable.isExistingCars(id, key.replace(CAR, ""))) {
                    addLocalItem(id, key, myMWRunnable.queryCars(id, key.replace(CAR, "")),
                            myMWRunnable.queryCarsPrice(id, key.replace(CAR, "")),
                            myMWRunnable.queryCarsReserved(id, key.replace(CAR, "")), null);
                    Car localCar = (Car) readData(id, key);
                    if (localCar.getCount() >= count) {
                        localCar.setReserved(localCar.getReserved() + count);
                        localCar.setCount(localCar.getCount() - count);
                        return true;
                    } else {
                        Trace.error("cars fully booked, cannot make reservation");
                        return false;
                    }
                } else {
                    Trace.error("car item does not exist, cannot increase item Count");
                    return false;
                }
            } else if (key.contains(ROOM)) {
                if (myMWRunnable.isExistingRooms(id, key.replace(ROOM, ""))) {
                    addLocalItem(id, key, myMWRunnable.queryRooms(id, key.replace(ROOM, "")),
                            myMWRunnable.queryRoomsPrice(id, key.replace(ROOM, "")),
                            myMWRunnable.queryRoomsReserved(id, key.replace(ROOM, "")), null);
                    Room localRoom = (Room) readData(id, key);
                    if (localRoom.getCount() >= count) {
                        localRoom.setReserved(localRoom.getReserved() + count);
                        localRoom.setCount(localRoom.getCount() - count);
                        return true;
                    } else {
                        Trace.error("rooms fully booked, cannot make reservation");
                        return false;
                    }
                } else {
                    Trace.info("room item does not exist, cannot increase item Count");
                    return false;
                }
            }
        } else {
            if (item.getCount() >= count) {
                item.setReserved(item.getReserved() + count);
                item.setCount(item.getCount() - count);
                Trace.info("item reserved: " + item.getReserved() + "    item count: " + item.getCount());
                return true;
            } else if (item.getCount() == -1) {
                Trace.error("item does not exist, failed to perform reserveItem");
                return false;
            } else {
                Trace.error(key.replace("-", "") + " fully booked, cannot make reservation");
                return false;
            }
        }
        return false;
    }
}