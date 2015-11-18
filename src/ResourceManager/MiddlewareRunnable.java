package ResourceManager;


import TransactionManager.TransactionManager;
import LockManager.*;

import java.io.*;
import java.net.*;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;


/**
 * Class that implements a client connection to the Middleware TCP server
 */
public class MiddlewareRunnable implements Runnable, ResourceManager {
    Socket clientSocket = null;
    Socket carSocket = null;
    Socket flightSocket = null;
    Socket roomSocket = null;
    public PrintWriter toCar, toFlight, toRoom, toClient;
    public BufferedReader fromCar, fromFlight, fromRoom, fromClient;
    TransactionManager tm;

    public MiddlewareRunnable(Socket clientSocket) {
        this.clientSocket = clientSocket;
        tm = new TransactionManager(this);
        connectRM();
        setComms();
    }

    private void setComms() {
        try {
            toClient = new PrintWriter(clientSocket.getOutputStream(), true);
            toCar = new PrintWriter(carSocket.getOutputStream(), true);
            toFlight = new PrintWriter(flightSocket.getOutputStream(), true);
            toRoom = new PrintWriter(roomSocket.getOutputStream(), true);
            fromClient = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            fromCar = new BufferedReader(new InputStreamReader(carSocket.getInputStream()));
            fromFlight = new BufferedReader(new InputStreamReader(flightSocket.getInputStream()));
            fromRoom = new BufferedReader(new InputStreamReader(roomSocket.getInputStream()));
        } catch (IOException e) {
            Trace.info("Cannot establish comms with an end-user client or with one of the RMs");
        }
    }

    private void connectRM() {
        try {
            this.flightSocket = new Socket(TCPServer.rmAddresses[0], Integer.parseInt(TCPServer.rmAddresses[1]));
            this.carSocket = new Socket(TCPServer.rmAddresses[2], Integer.parseInt(TCPServer.rmAddresses[3]));
            this.roomSocket = new Socket(TCPServer.rmAddresses[4], Integer.parseInt(TCPServer.rmAddresses[5]));
        } catch (IOException e) {
            Trace.info("Cannot connect to all 3 mandatory RMs");
        }
    }


    public void run() {
        try {
            String inputLine;

            //keep on prompting user for input until disconnect
            outerloop:
            while ((inputLine = fromClient.readLine()) != null) {
                //split the line into tokens and save them into an array
                String[] cmdWords = inputLine.split(",");
                int choice = findChoice(cmdWords);
                boolean success;
                int value;
                String stringValue;
                //switch statement based on 1st keyword of user input
                switch (choice) {
                    case 2:
                        if (cmdWords.length < 4) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction()) {
                            success = addFlight(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]), Integer.parseInt(cmdWords[3]), Integer.parseInt(cmdWords[4]));
                        } else
                            success = tm.addFlight(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]), Integer.parseInt(cmdWords[3]), Integer.parseInt(cmdWords[4]));
                        if (success) {
                            toClient.println("true");
                            Trace.info("RM addFlight successful");
                        } else {
                            toClient.println("false");
                            Trace.info("RM addFlight failed");
                        }
                        break;
                    case 3:
                        if (cmdWords.length < 4) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            success = addCars(Integer.parseInt(cmdWords[1]), cmdWords[2], Integer.parseInt(cmdWords[3]), Integer.parseInt(cmdWords[4]));
                        else
                            success = tm.addCars(Integer.parseInt(cmdWords[1]), cmdWords[2], Integer.parseInt(cmdWords[3]), Integer.parseInt(cmdWords[4]));
                        if (success) toClient.println("true");
                        else toClient.println("false");
                        break;
                    case 4:
                        if (cmdWords.length < 4) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            success = addRooms(Integer.parseInt(cmdWords[1]), cmdWords[2], Integer.parseInt(cmdWords[3]), Integer.parseInt(cmdWords[4]));
                        else
                            success = tm.addRooms(Integer.parseInt(cmdWords[1]), cmdWords[2], Integer.parseInt(cmdWords[3]), Integer.parseInt(cmdWords[4]));
                        if (success) {
                            toClient.println("true");
                        } else {
                            toClient.println("false");
                        }
                        break;
                    case 5:
                        if (cmdWords.length < 1) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            value = newCustomer(Integer.parseInt(cmdWords[1]));
                        else value = tm.newCustomer(Integer.parseInt(cmdWords[1]));
                        toClient.println(value);
                        break;
                    case 6:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            success = deleteFlight(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        else success = tm.deleteFlight(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        if (success) toClient.println("true");
                        else toClient.println("false");

                        break;
                    case 7:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            success = deleteCars(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        else success = tm.deleteCars(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        if (success) toClient.println("true");
                        else toClient.println("false");

                        break;
                    case 8:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            success = deleteRooms(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        else success = tm.deleteRooms(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        if (success) toClient.println("true");
                        else toClient.println("false");

                        break;
                    case 9:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            success = deleteCustomer(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        else success = tm.deleteCustomer(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        if (success) toClient.println("true");
                        else toClient.println("false");
                        break;
                    case 10:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            value = queryFlight(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        else value = tm.queryFlight(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        toClient.println(value);
                        break;
                    case 11:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            value = queryCars(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        else value = tm.queryCars(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        toClient.println(value);
                        break;
                    case 12:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            value = queryRooms(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        else value = tm.queryRooms(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        toClient.println(value);
                        break;
                    case 13:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            stringValue = queryCustomerInfo(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        else
                            stringValue = tm.queryCustomerInfo(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        toClient.println(stringValue);
                        break;
                    case 14:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            value = queryFlightPrice(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        else value = tm.queryFlightPrice(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        toClient.println(value);
                        break;
                    case 15:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            value = queryCarsPrice(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        else value = tm.queryCarsPrice(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        toClient.println(value);
                        break;
                    case 16:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            value = queryRoomsPrice(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        else value = tm.queryRoomsPrice(Integer.parseInt(cmdWords[1]), cmdWords[2]);
                        toClient.println(value);
                        break;
                    case 17:
                        if (cmdWords.length < 3) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        System.out.println(Integer.parseInt(cmdWords[2]));
                        System.out.println(Integer.parseInt(cmdWords[3]));
                        if (!tm.isInTransaction())
                            success = reserveFlight(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]), Integer.parseInt(cmdWords[3]));
                        else
                            success = tm.reserveFlight(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]), Integer.parseInt(cmdWords[3]));
                        if (success) toClient.println("true");
                        else toClient.println("false");
                        break;
                    case 18:
                        if (cmdWords.length < 3) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            success = reserveCar(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]), cmdWords[3]);
                        else
                            success = tm.reserveCar(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]), cmdWords[3]);
                        if (success) toClient.println("true");
                        else toClient.println("false");
                        break;
                    case 19:
                        if (cmdWords.length < 3) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            success = reserveRoom(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]), cmdWords[3]);
                        else
                            success = tm.reserveRoom(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]), cmdWords[3]);
                        if (success) toClient.println("true");
                        else toClient.println("false");
                        break;
                    case 20:
                        int numFlights = Integer.parseInt(cmdWords[1]);
                        int id = Integer.parseInt(cmdWords[2]);
                        int customerId = Integer.parseInt(cmdWords[3]);
                        Vector flightNumbers = new Vector();
                        for (int i = 0; i < numFlights; i++) {
                            flightNumbers.addElement(Integer.parseInt(cmdWords[i + 4]));
                        }
                        String location = cmdWords[3 + numFlights + 1];
                        boolean car = cmdWords[3 + numFlights + 2].contains("true");
                        boolean room = cmdWords[3 + numFlights + 3].contains("true");
                        if (!tm.isInTransaction())
                            success = reserveItinerary(id, customerId, flightNumbers, location, car, room);
                        else success = tm.reserveItinerary(id, customerId, flightNumbers, location, car, room);
                        if (success) toClient.println("true");
                        else toClient.println("false");
                        break;
                    case 21:
                        break outerloop;
                    case 22:
                        if (cmdWords.length < 2) {
                            toClient.println("ERROR : wrong arguments");
                            break;
                        }
                        if (!tm.isInTransaction())
                            success = newCustomerId(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        else success = tm.newCustomerId(Integer.parseInt(cmdWords[1]), Integer.parseInt(cmdWords[2]));
                        toClient.println(success);
                        break;
                    case 23:
                        if (tm.start()) {
                            toClient.println("transaction started with XID: " + tm.getCurrentActiveTransactionID());
                        } else toClient.println("an existing transaction is currently active");
                        break;
                    case 24:
                        if (tm.isInTransaction()) {
                            if (tm.abort()) {
                                toClient.println("transaction successfully aborted");
                                System.out.println("transaction aborted");
                            } else {
                                toClient.println("transaction abort encountered undo failure");
                                System.out.println("transaction aborted with undo failure");
                            }
                        } else {
                            toClient.println("no open transaction currently, nothing to abort");
                            System.out.println("nothing to abort");
                        }
                        break;
                    case 25:
                        if (tm.commit()) {
                            toClient.println("transaction successfully committed");
                        } else toClient.println("transaction commit error, transaction aborted");
                        break;
                    case 66:
                        if (TransactionManager.noActiveTransactions()) {
                            toFlight.println("shutdown");
                            toCar.println("shutdown");
                            toRoom.println("shutdown");
                            toClient.println("true");
                            System.exit(0);
                        } else {
                            toClient.println("false");
                        }
                        break;
                    default:
                        toClient.println("ERROR :  Command " + cmdWords[0] + " not supported");
                        break;
                }
            }
            toCar.println("END");
            toRoom.println("END");
            toFlight.println("END");
            Trace.info("an end-user client disconnected");
            fromClient.close();
            fromCar.close();
            fromRoom.close();
            fromFlight.close();
            toClient.close();
            toCar.close();
            toRoom.close();
            toFlight.close();
        } catch (IOException e) {
            System.out.println("exception IO");
        }
    }

    public int findChoice(String[] cmdWords) {
        int choice = -1;

        if (cmdWords[0].compareToIgnoreCase("help") == 0)
            choice = 1;
        else if (cmdWords[0].compareToIgnoreCase("addflight") == 0)
            choice = 2;
        else if (cmdWords[0].compareToIgnoreCase("addcars") == 0)
            choice = 3;
        else if (cmdWords[0].compareToIgnoreCase("addrooms") == 0)
            choice = 4;
        else if (cmdWords[0].compareToIgnoreCase("newcustomer") == 0)
            choice = 5;
        else if (cmdWords[0].compareToIgnoreCase("deleteflight") == 0)
            choice = 6;
        else if (cmdWords[0].compareToIgnoreCase("deletecars") == 0)
            choice = 7;
        else if (cmdWords[0].compareToIgnoreCase("deleterooms") == 0)
            choice = 8;
        else if (cmdWords[0].compareToIgnoreCase("deletecustomer") == 0)
            choice = 9;
        else if (cmdWords[0].compareToIgnoreCase("queryflight") == 0)
            choice = 10;
        else if (cmdWords[0].compareToIgnoreCase("querycars") == 0)
            choice = 11;
        else if (cmdWords[0].compareToIgnoreCase("queryrooms") == 0)
            choice = 12;
        else if (cmdWords[0].compareToIgnoreCase("querycustomerinfo") == 0)
            choice = 13;
        else if (cmdWords[0].compareToIgnoreCase("queryflightprice") == 0)
            choice = 14;
        else if (cmdWords[0].compareToIgnoreCase("querycarsprice") == 0)
            choice = 15;
        else if (cmdWords[0].compareToIgnoreCase("queryroomsprice") == 0)
            choice = 16;
        else if (cmdWords[0].compareToIgnoreCase("reserveflight") == 0)
            choice = 17;
        else if (cmdWords[0].compareToIgnoreCase("reservecar") == 0)
            choice = 18;
        else if (cmdWords[0].compareToIgnoreCase("reserveroom") == 0)
            choice = 19;
        else if (cmdWords[0].compareToIgnoreCase("reserveitinerary") == 0)
            choice = 20;
        else if (cmdWords[0].compareToIgnoreCase("END") == 0)
            choice = 21;
        else if (cmdWords[0].compareToIgnoreCase("newcustomerid") == 0)
            choice = 22;
        else if (cmdWords[0].compareToIgnoreCase("start") == 0)
            return 23;
        else if (cmdWords[0].compareToIgnoreCase("abort") == 0)
            return 24;
        else if (cmdWords[0].compareToIgnoreCase("commit") == 0)
            return 25;
        else if (cmdWords[0].compareToIgnoreCase("shutdown") == 0)
            return 66;
        else
            choice = -1;
        return choice;
    }


    // Basic operations on ResourceManager.RMItem //

    // Read a data item.
    public RMItem readData(int id, String key) {
        synchronized (TCPServer.m_itemHT_customer) {
            return (RMItem) TCPServer.m_itemHT_customer.get(key);
        }
    }

    // Write a data item.
    private void writeData(int id, String key, RMItem value) {
        synchronized (TCPServer.m_itemHT_customer) {
            TCPServer.m_itemHT_customer.put(key, value);
        }
    }

    // Remove the item out of storage.
    protected RMItem removeData(int id, String key) {
        synchronized (TCPServer.m_itemHT_customer) {
            return (RMItem) TCPServer.m_itemHT_customer.remove(key);
        }
    }


    // Basic operations on ResourceManager.ReservableItem //

    // Delete the entire item.
    protected boolean deleteItem(int id, String key) {
        Trace.info("RM::deleteItem(" + id + ", " + key + ") called.");
        ReservableItem curObj = (ReservableItem) readData(id, key);
        // Check if there is such an item in the storage.
        if (curObj == null) {
            Trace.warn("RM::deleteItem(" + id + ", " + key + ") failed: "
                    + " item doesn't exist.");
            return false;
        } else {
            if (curObj.getReserved() == 0) {
                removeData(id, curObj.getKey());
                Trace.info("RM::deleteItem(" + id + ", " + key + ") OK.");
                return true;
            } else {
                Trace.info("RM::deleteItem(" + id + ", " + key + ") failed: "
                        + "some customers have reserved it.");
                return false;
            }
        }
    }

    // Query the number of available seats/rooms/cars.
    protected int queryNum(int id, String key) {
        Trace.info("RM::queryNum(" + id + ", " + key + ") called.");
        ReservableItem curObj = (ReservableItem) readData(id, key);
        int value = 0;
        if (curObj != null) {
            value = curObj.getCount();
        }
        Trace.info("RM::queryNum(" + id + ", " + key + ") OK: " + value);
        return value;
    }

    // Query the price of an item.
    protected int queryPrice(int id, String key) {
        Trace.info("RM::queryPrice(" + id + ", " + key + ") called.");
        ReservableItem curObj = (ReservableItem) readData(id, key);
        int value = 0;
        if (curObj != null) {
            value = curObj.getPrice();
        }
        Trace.info("RM::queryPrice(" + id + ", " + key + ") OK: $" + value);
        return value;
    }


    public boolean unreserveItem(int id, int customerId, String key, String location) throws IOException {
        Trace.info("RM::reserveItem(" + id + ", " + customerId + ", "
                + key + ", " + location + ") called.");
        // Read customer object if it exists (and read lock it).
        Customer cust = (Customer) readData(id, Customer.getKey(customerId));
        if (cust == null) {
            Trace.warn("RM::reserveItem(" + id + ", " + customerId + ", "
                    + key + ", " + location + ") failed: customer doesn't exist.");
            return false;
        }
        //Check for item availability and getting price
        boolean isSuccessfulReservation = false;
        int itemPrice = -1;
        if (key.contains("car-")) {
            toCar.println("increaseReservableItemCount," + id + "," + key + "," + "1");
            if (fromCar.readLine().contains("true")) {
                isSuccessfulReservation = true;
                toCar.println("queryCarsPrice," + id + "," + location);
                itemPrice = Integer.parseInt(fromCar.readLine());
            }
        } else if (key.contains("flight-")) {
            toFlight.println("increaseReservableItemCount," + id + "," + key + "," + "1");
            if (fromFlight.readLine().contains("true")) {
                isSuccessfulReservation = true;
                toFlight.println("queryFlightPrice," + id + "," + location);
                itemPrice = Integer.parseInt(fromFlight.readLine());
            }
        } else if (key.contains("room-")) {
            toRoom.println("increaseReservableItemCount," + id + "," + key + "," + "1");
            if (fromRoom.readLine().contains("true")) {
                isSuccessfulReservation = true;
                toRoom.println("queryRoomsPrice," + id + "," + location);
                itemPrice = Integer.parseInt(fromRoom.readLine());
            }
        } else {
            ;
        }
        // Check if the item is available.
        if (!isSuccessfulReservation) {
            Trace.warn("RM::reserveItem(" + id + ", " + customerId + ", "
                    + key + ", " + location + ") failed: item doesn't exist or no more items.");
            return false;
        } else {
            // Do reservation.

            cust.unReserve(key);
            //this should be redundant code
            writeData(id, cust.getKey(), cust);

            Trace.warn("RM::reserveItem(" + id + ", " + customerId + ", "
                    + key + ", " + location + ") OK.");
            return true;
        }
    }

    // Reserve an item.
    protected boolean reserveItem(int id, int customerId,
                                  String key, String location) throws Exception {
        Trace.info("RM::reserveItem(" + id + ", " + customerId + ", "
                + key + ", " + location + ") called.");
        // Read customer object if it exists (and read lock it).
        Customer cust = (Customer) readData(id, Customer.getKey(customerId));
        if (cust == null) {
            Trace.warn("RM::reserveItem(" + id + ", " + customerId + ", "
                    + key + ", " + location + ") failed: customer doesn't exist.");
            return false;
        }
        //Check for item availability and getting price
        boolean isSuccessfulReservation = false;
        int itemPrice = -1;
        if (key.contains("car-")) {
            toCar.println("reserveCar," + id + "," + customerId + "," + location);
            if (fromCar.readLine().contains("true")) {
                isSuccessfulReservation = true;
                toCar.println("queryCarsPrice," + id + "," + location);
                itemPrice = Integer.parseInt(fromCar.readLine());
            }
        } else if (key.contains("flight-")) {
            toFlight.println("reserveFlight," + id + "," + customerId + "," + location);
            if (fromFlight.readLine().contains("true")) {
                isSuccessfulReservation = true;
                toFlight.println("queryFlightPrice," + id + "," + location);
                itemPrice = Integer.parseInt(fromFlight.readLine());
            }
        } else if (key.contains("room-")) {
            toRoom.println("reserveRoom," + id + "," + customerId + "," + location);
            if (fromRoom.readLine().contains("true")) {
                isSuccessfulReservation = true;
                toRoom.println("queryRoomsPrice," + id + "," + location);
                itemPrice = Integer.parseInt(fromRoom.readLine());
            }
        } else {
            throw new Exception("can't reserve this");
        }
        // Check if the item is available.
        if (!isSuccessfulReservation) {
            Trace.warn("RM::reserveItem(" + id + ", " + customerId + ", "
                    + key + ", " + location + ") failed: item doesn't exist or no more items.");
            return false;
        } else {
            // Do reservation.

            cust.reserve(key, location, itemPrice);
            //this should be redundant code
            writeData(id, cust.getKey(), cust);

            Trace.warn("RM::reserveItem(" + id + ", " + customerId + ", "
                    + key + ", " + location + ") OK.");
            return true;
        }
    }


    // ResourceManager.Flight operations //

    public boolean isExistingCustomer(int id, int customerId) {
        Trace.info("RM::isExistingCustomer(" + id + ", " + customerId);
        Customer curObj = (Customer) readData(id, Customer.getKey(customerId));
        if (curObj == null) {
            return false;
        } else return true;
    }

    public boolean isExistingFlight(int id, int flightNumber) {
        toFlight.println("isExistingFlight" + "," + id + "," + flightNumber);
        String line = null;
        try {
            line = fromFlight.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line.equalsIgnoreCase("true");
    }

    // Create a new flight, or add seats to existing flight.
    // Note: if flightPrice <= 0 and the flight already exists, it maintains
    // its current price.
    @Override
    public boolean addFlight(int id, int flightNumber,
                             int numSeats, int flightPrice) {
        toFlight.println("addFlight" + "," + id + "," + +flightNumber + "," + numSeats + "," + flightPrice);
        String line = null;
        try {
            line = fromFlight.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line.equalsIgnoreCase("true")) {
            Trace.info(line);
            return true;
        } else {
            Trace.info(line);
            return false;
        }
    }


    @Override
    public boolean deleteFlight(int id, int flightNumber) {
        toFlight.println("deleteFlight" + "," + id + "," + flightNumber);
        String line = null;
        try {
            line = fromFlight.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line.equalsIgnoreCase("true")) return true;
        else return false;
    }

    // Returns the number of empty seats on this flight.
    @Override
    public int queryFlight(int id, int flightNumber) {
        toFlight.println("queryFlight" + "," + id + "," + flightNumber);
        String line = null;
        try {
            line = fromFlight.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Integer.parseInt(line);
    }

    // Returns price of this flight.
    public int queryFlightPrice(int id, int flightNumber) {
        toFlight.println("queryFlightPrice" + "," + id + "," + flightNumber);
        String line = null;
        try {
            line = fromFlight.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Integer.parseInt(line);
    }

    public int queryFlightReserved(int id, int flightNumber) {
        toFlight.println("queryFlightReserved" + "," + id + "," + flightNumber);
        String line = null;
        try {
            line = fromFlight.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Integer.parseInt(line);
    }

    /*
    // Returns the number of reservations for this flight.
    public int queryFlightReservations(int id, int flightNumber) {
        ResourceManager.Trace.info("RM::queryFlightReservations(" + id
                + ", #" + flightNumber + ") called.");
        ResourceManager.RMInteger numReservations = (ResourceManager.RMInteger) readData(id,
                ResourceManager.Flight.getNumReservationsKey(flightNumber));
        if (numReservations == null) {
            numReservations = new ResourceManager.RMInteger(0);
       }
        ResourceManager.Trace.info("RM::queryFlightReservations(" + id +
                ", #" + flightNumber + ") = " + numReservations);
        return numReservations.getValue();
    }
    */

    /*
    // Frees flight reservation record. ResourceManager.Flight reservation records help us
    // make sure we don't delete a flight if one or more customers are
    // holding reservations.
    public boolean freeFlightReservation(int id, int flightNumber) {
        ResourceManager.Trace.info("RM::freeFlightReservations(" + id + ", "
                + flightNumber + ") called.");
        ResourceManager.RMInteger numReservations = (ResourceManager.RMInteger) readData(id,
                ResourceManager.Flight.getNumReservationsKey(flightNumber));
        if (numReservations != null) {
            numReservations = new ResourceManager.RMInteger(
                    Math.max(0, numReservations.getValue() - 1));
        }
        writeData(id, ResourceManager.Flight.getNumReservationsKey(flightNumber), numReservations);
        ResourceManager.Trace.info("RM::freeFlightReservations(" + id + ", "
                + flightNumber + ") OK: reservations = " + numReservations);
        return true;
    }
    */


    // ResourceManager.Car operations //

    public boolean isExistingCars(int id, String location) {
        toCar.println("isExistingCars" + "," + id + "," + location);
        String line = null;
        try {
            line = fromCar.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line.equalsIgnoreCase("true");
    }

    // Create a new car location or add cars to an existing location.
    // Note: if price <= 0 and the car location already exists, it maintains
    // its current price.
    @Override
    public boolean addCars(int id, String location, int numCars, int carPrice) {
        toCar.println("addCars" + "," + id + "," + location + "," + numCars + "," + carPrice);
        String line = null;
        try {
            line = fromCar.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line.equalsIgnoreCase("true")) return true;
        else return false;
    }

    // Delete cars from a location.
    @Override
    public boolean deleteCars(int id, String location) {

        toCar.println("deleteCars" + "," + id + "," + location);
        String line = null;
        try {
            line = fromCar.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line.equalsIgnoreCase("true")) return true;
        else return false;
    }

    // Returns the number of cars available at a location.
    @Override
    public int queryCars(int id, String location) {
        toCar.println("queryCars" + "," + id + "," + location);
        String line = null;
        try {
            line = fromCar.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Integer.parseInt(line);
    }

    // Returns price of cars at this location.
    @Override
    public int queryCarsPrice(int id, String location) {
        toCar.println("queryCarsPrice" + "," + id + "," + location);
        String line = null;
        try {
            line = fromCar.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Integer.parseInt(line);
    }

    public int queryCarsReserved(int id, String location) {
        toCar.println("queryCarsReserved" + "," + id + "," + location);
        String line = null;
        try {
            line = fromCar.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Integer.parseInt(line);
    }


    // ResourceManager.Room operations //

    public boolean isExistingRooms(int id, String location) {
        toRoom.println("isExistingRooms" + "," + id + "," + location);
        String line = null;
        try {
            line = fromRoom.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line.equalsIgnoreCase("true");
    }

    // Create a new room location or add rooms to an existing location.
    // Note: if price <= 0 and the room location already exists, it maintains
    // its current price.
    @Override
    public boolean addRooms(int id, String location, int numRooms, int roomPrice) {
        toRoom.println("addRooms" + "," + id + "," + location + "," + numRooms + "," + roomPrice);
        String line = null;
        try {
            line = fromRoom.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (line.equalsIgnoreCase("true"));
    }

    // Delete rooms from a location.
    @Override
    public boolean deleteRooms(int id, String location) {
        toRoom.println("deleteRooms" + "," + id + "," + location);
        String line = null;
        try {
            line = fromRoom.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line.equalsIgnoreCase("true")) return true;
        else return false;
    }

    // Returns the number of rooms available at a location.
    @Override
    public int queryRooms(int id, String location) {
        toRoom.println("queryRooms" + "," + id + "," + location);
        String line = null;
        try {
            line = fromRoom.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Integer.parseInt(line);
    }

    // Returns room price at this location.
    @Override
    public int queryRoomsPrice(int id, String location) {
        toRoom.println("queryRoomsPrice" + "," + id + "," + location);
        String line = null;
        try {
            line = fromRoom.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Integer.parseInt(line);
    }

    public int queryRoomsReserved(int id, String location) {
        toRoom.println("queryRoomsReserved" + "," + id + "," + location);
        String line = null;
        try {
            line = fromRoom.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Integer.parseInt(line);
    }


    // ResourceManager.Customer operations //

    @Override
    public int newCustomer(int id) {
        Trace.info("INFO: RM::newCustomer(" + id + ") called.");
        // Generate a globally unique Id for the new customer.
        int customerId = Integer.parseInt(String.valueOf(id) +
                String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND)) +
                String.valueOf(Math.round(Math.random() * 100 + 1)));
        try {
            TCPServer.lm.Lock(id, "customer-"+customerId,LockManager.WRITE);
        } catch (DeadlockException e) {
            e.printStackTrace();
            tm.abort();
            return -2;
        }
        Customer cust = new Customer(customerId);
        writeData(id, cust.getKey(), cust);
        Trace.info("RM::newCustomer(" + id + ") OK: " + customerId);
        return customerId;
    }

    // This method makes testing easier.
    @Override
    public boolean newCustomerId(int id, int customerId) {
        Trace.info("INFO: RM::newCustomer(" + id + ", " + customerId + ") called.");
        Customer cust = (Customer) readData(id, Customer.getKey(customerId));
        if (cust == null) {
            cust = new Customer(customerId);
            writeData(id, cust.getKey(), cust);
            Trace.info("INFO: RM::newCustomer(" + id + ", " + customerId + ") OK.");
            return true;
        } else {
            Trace.info("INFO: RM::newCustomer(" + id + ", " +
                    customerId + ") failed: customer already exists.");
            return false;
        }
    }

    // Delete customer from the database.
    @Override
    public boolean deleteCustomer(int id, int customerId) {
        Trace.info("RM::deleteCustomer(" + id + ", " + customerId + ") called.");
        Customer cust = (Customer) readData(id, Customer.getKey(customerId));
        if (cust == null) {
            Trace.warn("RM::deleteCustomer(" + id + ", "
                    + customerId + ") failed: customer doesn't exist.");
            return false;
        } else {

            boolean reservableItemUpdated = true;
            // Increase the reserved numbers of all reservable items that
            // the customer reserved.
            RMHashtable reservationHT = cust.getReservations();
            for (Enumeration e = reservationHT.keys(); e.hasMoreElements(); ) {
                String reservedKey = (String) (e.nextElement());
                ReservedItem reservedItem = cust.getReservedItem(reservedKey);
                Trace.info("RM::deleteCustomer(" + id + ", " + customerId + "): "
                        + "deleting " + reservedItem.getCount() + " reservations "
                        + "for item " + reservedItem.getKey());
                if (reservedItem.getKey().contains("flight-")) {
                    toFlight.println("increaseReservableItemCount," + id + "," + reservedItem.getKey() + "," + reservedItem.getCount());
                    try {
                        String line = fromFlight.readLine();
                        if (line.contains("true")) {

                            Trace.info("reserved item increased");
                        } else {
                            Trace.info("reserved item count cannot be increased");
                            reservableItemUpdated = false;
                        }
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                } else if (reservedItem.getKey().contains("car-")) {
                    toCar.println("increaseReservableItemCount," + id + "," + reservedItem.getKey() + "," + reservedItem.getCount());
                    try {
                        String line = fromCar.readLine();
                        if (line.contains("true")) {
                            Trace.info("reserved item increased");
                        } else {
                            Trace.info("reserved item count cannot be increased");
                            reservableItemUpdated = false;

                        }
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }

                } else if (reservedItem.getKey().contains("room")) {
                    toRoom.println("increaseReservableItemCount," + id + "," + reservedItem.getKey() + "," + reservedItem.getCount());
                    try {
                        String line = fromRoom.readLine();
                        if (line.contains("true")) {
                            Trace.info("reserved item increased");
                        } else {
                            Trace.info("reserved item count cannot be increased");
                            reservableItemUpdated = false;

                        }
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }

                } else {
                    Trace.info("reserved item does not exist");
                }
            }
            if (reservableItemUpdated == true) {
                removeData(id, cust.getKey());
                Trace.info("RM::deleteCustomer(" + id + ", " + customerId + ") OK.");
                return true;
            }

            Trace.info("RM::deleteCustomer(" + id + ", " + customerId + ") failed. one of the reservedItem could not be updated");
            return false;
            // Remove the customer from the storage.
        }
    }

    public boolean getLockforCustomer(int id, int customerId) {
        Customer cust = (Customer) readData(id, Customer.getKey(customerId));
        if (cust == null) {
            return false;
        } else {
            boolean success = true;

            RMHashtable reservationHT = cust.getReservations();
            for (Enumeration e = reservationHT.keys(); e.hasMoreElements(); ) {
                String reservedKey = (String) (e.nextElement());
                ReservedItem reservedItem = cust.getReservedItem(reservedKey);
                try {
                    if (reservedItem.getKey().contains("flight-")) {
                        if (!TCPServer.lm.Lock(id, reservedItem.getKey(), LockManager.WRITE)) success = false;
                    } else if (reservedItem.getKey().contains("car-")) {
                        if (!TCPServer.lm.Lock(id, reservedItem.getKey(), LockManager.WRITE)) success = false;

                    } else if (reservedItem.getKey().contains("room")) {
                        if (TCPServer.lm.Lock(id, reservedItem.getKey(), LockManager.WRITE)) success = false;

                    } else {
                        Trace.info("reserved item does not exist");
                        return false;
                    }
                    return success;

                } catch (DeadlockException e1) {
                    tm.abort();
                    e1.printStackTrace();
                    return false;
                }
            }
            return success;

        }
    }

    public boolean undoDeleteCustomer(int id, int customerId) {
        Customer targetCust = null;
        for (int i = 0; i < tm.customers.size(); i++) {
            if (tm.customers.get(i).getId() == customerId) {
                targetCust = tm.customers.get(i);
            }
        }
        writeData(id, targetCust.getKey(), targetCust);
        RMHashtable reservationHT = targetCust.getReservations();
        for (Enumeration e = reservationHT.keys(); e.hasMoreElements(); ) {
            String reservedKey = (String) (e.nextElement());
            ReservedItem reservedItem = targetCust.getReservedItem(reservedKey);
            if (reservedItem.getKey().contains("flight-")) {
                toFlight.println("decreaseReservableItemCount," + id + "," + reservedItem.getKey() + "," + reservedItem.getCount());
                try {
                    String line = fromFlight.readLine();
                    if (line.contains("true")) {

                        Trace.info("reserved item increased");
                    } else {
                        Trace.info("reserved item count cannot be increased");
                    }
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            } else if (reservedItem.getKey().contains("car-")) {
                toCar.println("decreaseReservableItemCount," + id + "," + reservedItem.getKey() + "," + reservedItem.getCount());
                try {
                    String line = fromCar.readLine();
                    if (line.contains("true")) {
                        Trace.info("reserved item increased");
                    } else {
                        Trace.info("reserved item count cannot be increased");

                    }
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

            } else if (reservedItem.getKey().contains("room")) {
                toRoom.println("decreaseReservableItemCount," + id + "," + reservedItem.getKey() + "," + reservedItem.getCount());
                try {
                    String line = fromRoom.readLine();
                    if (line.contains("true")) {
                        Trace.info("reserved item increased");
                    } else {
                        Trace.info("reserved item count cannot be increased");

                    }
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

            } else {
                Trace.info("reserved item does not exist");
            }
        }

        return true;
    }


    // Return data structure containing customer reservation info.
    // Returns null if the customer doesn't exist.
    // Returns empty ResourceManager.RMHashtable if customer exists but has no reservations.
    public RMHashtable getCustomerReservations(int id, int customerId) {
        Trace.info("RM::getCustomerReservations(" + id + ", "
                + customerId + ") called.");
        Customer cust = (Customer) readData(id, Customer.getKey(customerId));
        if (cust == null) {
            Trace.info("RM::getCustomerReservations(" + id + ", "
                    + customerId + ") failed: customer doesn't exist.");
            return null;
        } else {
            return cust.getReservations();
        }
    }

    // Return a bill.
    @Override
    public String queryCustomerInfo(int id, int customerId) {
        Trace.info("RM::queryCustomerInfo(" + id + ", " + customerId + ") called.");
        Customer cust = (Customer) readData(id, Customer.getKey(customerId));
        if (cust == null) {
            Trace.warn("RM::queryCustomerInfo(" + id + ", "
                    + customerId + ") failed: customer doesn't exist.");
            // Returning an empty bill means that the customer doesn't exist.
            return "customer does not exist";
        } else {
            String s = cust.printBill();
            Trace.info("RM::queryCustomerInfo(" + id + ", " + customerId + "): \n");
            System.out.println(s);
            return s;
        }
    }

    // Add flight reservation to this customer.
    @Override
    public boolean reserveFlight(int id, int customerId, int flightNumber) {
        try {
            return reserveItem(id, customerId,
                    Flight.getKey(flightNumber), String.valueOf(flightNumber));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // Add car reservation to this customer.
    @Override
    public boolean reserveCar(int id, int customerId, String location) {
        try {
            return reserveItem(id, customerId, Car.getKey(location), location);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // Add room reservation to this customer.
    @Override
    public boolean reserveRoom(int id, int customerId, String location) {
        try {
            return reserveItem(id, customerId, Room.getKey(location), location);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    // Reserve an itinerary.
    @Override
    public boolean reserveItinerary(int id, int customerId, Vector flightNumbers,
                                    String location, boolean car, boolean room) {

        TransactionManager localTM = new TransactionManager(MiddlewareRunnable.this);
        localTM.start();
        try {
            TCPServer.lm.Lock(localTM.getCurrentActiveTransactionID(), localTM.CUSTOMER, LockManager.WRITE);
            TCPServer.lm.Lock(localTM.getCurrentActiveTransactionID(), localTM.FLIGHT, LockManager.WRITE);
            TCPServer.lm.Lock(localTM.getCurrentActiveTransactionID(), localTM.CAR, LockManager.WRITE);
            TCPServer.lm.Lock(localTM.getCurrentActiveTransactionID(), localTM.ROOM, LockManager.WRITE);
            Iterator it = flightNumbers.iterator();

            boolean isSuccessfulReservation = true;
            while (it.hasNext()) {
                try {
                    if (!localTM.reserveFlight(localTM.getCurrentActiveTransactionID(), customerId, getInt(it.next()))) {
                        isSuccessfulReservation = false;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (car) {
                if (!localTM.reserveCar(localTM.getCurrentActiveTransactionID(), customerId, location)) {
                    isSuccessfulReservation = false;
                }
            }
            if (room) {
                if (!localTM.reserveRoom(localTM.getCurrentActiveTransactionID(), customerId, location)) {
                    isSuccessfulReservation = false;
                }
            }

            if (isSuccessfulReservation) localTM.commit();
            else localTM.abort();
            return isSuccessfulReservation;
        } catch (DeadlockException e) {
            localTM.abort();
            return false;
        }


    }

    @Override
    public boolean increaseReservableItemCount(int id, String key, int count) {
        return false;
    }

    public int getInt(Object temp) throws Exception {
        try {
            return new Integer(temp.toString());
        } catch (Exception e) {
            throw e;
        }
    }

}