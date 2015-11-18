package ResourceManager;

import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.io.*;


public class Client {
    private String mwHost;
    private int mwPort;
    private Socket mwSocket;
    protected PrintWriter toMW;
    protected BufferedReader fromMW;


    public Client(String mwHost, int mwPort) {
        try {
            this.mwHost = mwHost;
            this.mwPort = mwPort;
            mwSocket = new Socket(mwHost, mwPort);
            toMW = new PrintWriter(mwSocket.getOutputStream(), true);
            fromMW = new BufferedReader(new InputStreamReader(mwSocket.getInputStream()));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {

            if (args.length != 2) {
                System.out.println("Usage: ResourceManager.Client <mwHost> <mwPort>");
                System.exit(-1);
            }

            String mwHost = args[0];
            int mwPort = Integer.parseInt(args[1]);

            Client client = new Client(mwHost, mwPort);
            client.run();


        } catch (NullPointerException e1) {
            System.out.println("Sorry: MiddleWare and RMs currently unavailable");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void run() {

        int id;
        int flightNumber;
        int flightPrice;
        int numSeats;
        boolean room;
        boolean car;
        int price;
        int numRooms;
        int numCars;
        String location;

        String command = "";
        Vector arguments = new Vector();

        BufferedReader stdin =
                new BufferedReader(new InputStreamReader(System.in));

        System.out.println("ResourceManager.Client Interface");
        System.out.println("Type \"help\" for list of supported commands");

        while (true) {

            try {
                //read the next command
                command = stdin.readLine();
            } catch (IOException io) {
                System.out.println("Unable to read from standard in");
                System.exit(1);
            }
            //remove heading and trailing white space
            command = command.trim();

            if (command.equals("")) continue;
            arguments = parse(command);


            //decide which of the commands this was
            switch (findChoice((String) arguments.elementAt(0))) {

                case 1: //help section
                    if (arguments.size() == 1)   //command was "help"
                        listCommands();
                    else if (arguments.size() == 2)  //command was "help <commandname>"
                        listSpecific((String) arguments.elementAt(1));
                    else  //wrong use of help command
                        System.out.println("Improper use of help command. Type help or help, <commandname>");
                    break;

                case 2:  //new flight
                    if (arguments.size() != 5) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Adding a new ResourceManager.Flight using id: " + arguments.elementAt(1));
                    System.out.println("ResourceManager.Flight number: " + arguments.elementAt(2));
                    System.out.println("Add ResourceManager.Flight Seats: " + arguments.elementAt(3));
                    System.out.println("Set ResourceManager.Flight Price: " + arguments.elementAt(4));

                    try {
                        id = getInt(arguments.elementAt(1));
                        flightNumber = getInt(arguments.elementAt(2));
                        numSeats = getInt(arguments.elementAt(3));
                        flightPrice = getInt(arguments.elementAt(4));

                        toMW.println("addFlight," + id + "," + flightNumber + "," + numSeats + "," + flightPrice);
                        String line = fromMW.readLine();
                        if (line.contains("true"))
                            Trace.info("MW addFlight successful");
                        else
                            Trace.info("MW addFlight failed");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 3:  //new car
                    if (arguments.size() != 5) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Adding a new car using id: " + arguments.elementAt(1));
                    System.out.println("car Location: " + arguments.elementAt(2));
                    System.out.println("Add Number of cars: " + arguments.elementAt(3));
                    System.out.println("Set Price: " + arguments.elementAt(4));
                    try {
                        id = getInt(arguments.elementAt(1));
                        location = getString(arguments.elementAt(2));
                        numCars = getInt(arguments.elementAt(3));
                        price = getInt(arguments.elementAt(4));
                        toMW.println("addCars," + id + "," + location + "," + numCars + "," + price);
                        if (fromMW.readLine().contains("true"))
                            Trace.info("MW addCars successful");
                        else
                            Trace.info("MW addCars failed");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 4:  //new room
                    if (arguments.size() != 5) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Adding a new room using id: " + arguments.elementAt(1));
                    System.out.println("room Location: " + arguments.elementAt(2));
                    System.out.println("Add Number of rooms: " + arguments.elementAt(3));
                    System.out.println("Set Price: " + arguments.elementAt(4));
                    try {
                        id = getInt(arguments.elementAt(1));
                        location = getString(arguments.elementAt(2));
                        numRooms = getInt(arguments.elementAt(3));
                        price = getInt(arguments.elementAt(4));

                        toMW.println("addRooms," + id + "," + location + "," + numRooms + "," + price);
                        if (fromMW.readLine().contains("true"))
                            Trace.info("MW addRooms successful");
                        else
                            Trace.info("MW addRooms failed");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 5:  //new ResourceManager.Customer
                    if (arguments.size() != 2) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Adding a new ResourceManager.Customer using id: " + arguments.elementAt(1));
                    try {
                        id = getInt(arguments.elementAt(1));
                        toMW.println("newCustomer," + id);
                        int customer = Integer.parseInt(fromMW.readLine());
                        if (customer==-2) {
                            Trace.info("MW newCustomer unsuccessful: failed to get lock");
                        } else {
                            Trace.info("new customer id: " + customer);
                            Trace.info("MW newCustomer successful");
                        }
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 6: //delete ResourceManager.Flight
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Deleting a flight using id: " + arguments.elementAt(1));
                    System.out.println("ResourceManager.Flight Number: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        flightNumber = getInt(arguments.elementAt(2));

                        toMW.println("deleteFlight," + id + "," + flightNumber);
                        if (fromMW.readLine().contains("true"))
                            Trace.info("MW deleteFlight successful");
                        else
                            Trace.info("MW deleteFlight failed");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        System.out.println("EXCEPTION: ");
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                    }
                    break;

                case 7: //delete car
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Deleting the cars from a particular location  using id: " + arguments.elementAt(1));
                    System.out.println("car Location: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        location = getString(arguments.elementAt(2));

                        toMW.println("deleteCars," + id + "," + location);
                        if (fromMW.readLine().contains("true"))
                            Trace.info("MW deleteCars successful");
                        else
                            Trace.info("cars could not be deleted");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 8: //delete room
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Deleting all rooms from a particular location  using id: " + arguments.elementAt(1));
                    System.out.println("room Location: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        location = getString(arguments.elementAt(2));

                        toMW.println("deleteRooms," + id + "," + location);
                        if (fromMW.readLine().contains("true"))
                            Trace.info("MW deleteRooms successful");
                        else
                            Trace.info("MW deleteRooms failed");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 9: //delete ResourceManager.Customer
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Deleting a customer from the database using id: " + arguments.elementAt(1));
                    System.out.println("ResourceManager.Customer id: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        int customer = getInt(arguments.elementAt(2));

                        toMW.println("deleteCustomer," + id + "," + customer);
                        if (fromMW.readLine().contains("true"))
                            Trace.info("MW deleteCustomer successful");
                        else
                            Trace.info("MW deleteCustomer failed");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 10: //querying a flight
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Querying a flight using id: " + arguments.elementAt(1));
                    System.out.println("ResourceManager.Flight number: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        flightNumber = getInt(arguments.elementAt(2));
                        toMW.println("queryFlight," + id + "," + flightNumber);
                        int seats = Integer.parseInt(fromMW.readLine());
                        Trace.info("MW Number of seats available: " + seats);
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 11: //querying a car Location
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Querying a car location using id: " + arguments.elementAt(1));
                    System.out.println("car location: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        location = getString(arguments.elementAt(2));
                        toMW.println("queryCars," + id + "," + location);

                        numCars = Integer.parseInt(fromMW.readLine());
                        Trace.info("MW number of cars at this location: " + numCars);
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 12: //querying a room location
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Querying a room location using id: " + arguments.elementAt(1));
                    System.out.println("room location: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        location = getString(arguments.elementAt(2));
                        toMW.println("queryRooms," + id + "," + location);
                        numRooms = Integer.parseInt(fromMW.readLine());
                        Trace.info("MW number of rooms at this location: " + numRooms);
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 13: //querying ResourceManager.Customer Information
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Querying ResourceManager.Customer information using id: " + arguments.elementAt(1));
                    System.out.println("ResourceManager.Customer id: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        int customer = getInt(arguments.elementAt(2));

                        toMW.println("queryCustomerInfo," + id + "," + customer);
                        String bill = fromMW.readLine();
                        Trace.info("MW ResourceManager.Customer info: " + bill);
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 14: //querying a flight Price
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Querying a flight Price using id: " + arguments.elementAt(1));
                    System.out.println("ResourceManager.Flight number: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        flightNumber = getInt(arguments.elementAt(2));
                        toMW.println("queryFlightPrice," + id + "," + flightNumber);
                        price = Integer.parseInt(fromMW.readLine());
                        Trace.info("MW Price of a seat: " + price);
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 15: //querying a car Price
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Querying a car price using id: " + arguments.elementAt(1));
                    System.out.println("car location: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        location = getString(arguments.elementAt(2));
                        toMW.println("queryCarsPrice," + id + "," + location);
                        price = Integer.parseInt(fromMW.readLine());
                        Trace.info("MW Price of a car at this location: " + price);
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 16: //querying a room price
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Querying a room price using id: " + arguments.elementAt(1));
                    System.out.println("room Location: " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        location = getString(arguments.elementAt(2));
                        toMW.println("queryRoomsPrice," + id + "," + location);

                        price = Integer.parseInt(fromMW.readLine());
                        Trace.info("MW Price of rooms at this location: " + price);
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 17:  //reserve a flight
                    if (arguments.size() != 4) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Reserving a seat on a flight using id: " + arguments.elementAt(1));
                    System.out.println("ResourceManager.Customer id: " + arguments.elementAt(2));
                    System.out.println("ResourceManager.Flight number: " + arguments.elementAt(3));
                    try {
                        id = getInt(arguments.elementAt(1));
                        int customer = getInt(arguments.elementAt(2));
                        flightNumber = getInt(arguments.elementAt(3));
                        toMW.println("reserveFlight," + id + "," + customer + "," + flightNumber);
                        if (fromMW.readLine().contains("true"))
                            Trace.info("MW reserveFlight successful");
                        else
                            Trace.info("MW reserveFlight failed");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 18:  //reserve a car
                    if (arguments.size() != 4) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Reserving a car at a location using id: " + arguments.elementAt(1));
                    System.out.println("ResourceManager.Customer id: " + arguments.elementAt(2));
                    System.out.println("Location: " + arguments.elementAt(3));
                    try {
                        id = getInt(arguments.elementAt(1));
                        int customer = getInt(arguments.elementAt(2));
                        location = getString(arguments.elementAt(3));
                        toMW.println("reserveCar," + id + "," + customer + "," + location);
                        if (fromMW.readLine().contains("true"))
                            Trace.info("MW reserveCar successful");
                        else
                            Trace.info("MW reserveCar failed");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 19:  //reserve a room
                    if (arguments.size() != 4) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Reserving a room at a location using id: " + arguments.elementAt(1));
                    System.out.println("ResourceManager.Customer id: " + arguments.elementAt(2));
                    System.out.println("Location: " + arguments.elementAt(3));
                    try {
                        id = getInt(arguments.elementAt(1));
                        int customer = getInt(arguments.elementAt(2));
                        location = getString(arguments.elementAt(3));
                        toMW.println("reserveRoom," + id + "," + customer + "," + location);
                        if (fromMW.readLine().contains("true"))
                            Trace.info("MW reserveRoom successful");
                        else
                            Trace.info("MW reserveRoom failed");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 20:  //reserve an Itinerary
                    if (arguments.size() < 7) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Reserving an Itinerary using id: " + arguments.elementAt(1));
                    System.out.println("ResourceManager.Customer id: " + arguments.elementAt(2));
                    for (int i = 0; i < arguments.size() - 6; i++)
                        System.out.println("ResourceManager.Flight number" + arguments.elementAt(3 + i));
                    System.out.println("Location for car/room booking: " + arguments.elementAt(arguments.size() - 3));
                    System.out.println("car to book?: " + arguments.elementAt(arguments.size() - 2));
                    System.out.println("room to book?: " + arguments.elementAt(arguments.size() - 1));
                    try {
                        id = getInt(arguments.elementAt(1));
                        int customer = getInt(arguments.elementAt(2));
                        Vector flightNumbers = new Vector();
                        for (int i = 0; i < arguments.size() - 6; i++)
                            flightNumbers.addElement(arguments.elementAt(3 + i));
                        location = getString(arguments.elementAt(arguments.size() - 3));
                        car = getBoolean(arguments.elementAt(arguments.size() - 2));
                        room = getBoolean(arguments.elementAt(arguments.size() - 1));
                        String cmd = "reserveItinerary" + "," + flightNumbers.size() + "," + id + "," + customer + ",";
                        for (int i = 0; i < arguments.size() - 6; i++) {
                            cmd = cmd + arguments.elementAt(3 + i) + ",";
                        }
                        toMW.println(cmd + location + "," + car + "," + room);
                        if (fromMW.readLine().contains("true"))
                            Trace.info("MW reserveItinerary successful");
                        else
                            Trace.info("MW reserveItinerary failed");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;

                case 21:  //quit the client
                    if (arguments.size() != 1) {
                        wrongNumber();
                        toMW.println("END");
                        break;
                    }
                    System.out.println("Quitting client.");
                    return;

                case 22:  //new ResourceManager.Customer given id
                    if (arguments.size() != 3) {
                        wrongNumber();
                        break;
                    }
                    System.out.println("Adding a new ResourceManager.Customer using id: "
                            + arguments.elementAt(1) + " and cid " + arguments.elementAt(2));
                    try {
                        id = getInt(arguments.elementAt(1));
                        int customer = getInt(arguments.elementAt(2));

                        toMW.println("newCustomerId," + id + "," + customer);

                        boolean c = fromMW.readLine().contains("true");
                        System.out.println("new customer id: " + customer);
                        if (c) Trace.info("MW newCustomerId successful");
                        else Trace.info("MW newCustomerId unsuccessful");
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (Exception e) {
                        System.out.println("EXCEPTION: ");
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                    }
                    break;

                case 23:
                    if (arguments.size() != 1) {
                        wrongNumber();
                        break;
                    }
                    try {
                        toMW.println("start");
                        System.out.println(fromMW.readLine());
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case 24:
                    if (arguments.size() != 1) {
                        wrongNumber();
                        break;
                    }
                    try {
                        toMW.println("abort");
                        System.out.println(fromMW.readLine());
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case 25:
                    if (arguments.size() != 1) {
                        wrongNumber();
                        break;
                    }
                    try {
                        toMW.println("commit");
                        System.out.println(fromMW.readLine());
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;

                case 66:
                    toMW.println("shutdown");
                    try {
                        if (fromMW.readLine().toLowerCase().contains("true"))
                            System.out.println("Reservation System shutdown successful");
                        else System.out.println("cannot shutdown system due to existing active transactions");
                        break;
                    } catch (NullPointerException e1) {
                        System.out.println("REQUEST FAILED: MW and RMs unavailable");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                default:
                    System.out.println("The interface does not support this command.");
                    break;
            }
        }
    }

    public Vector parse(String command) {
        Vector arguments = new Vector();
        StringTokenizer tokenizer = new StringTokenizer(command, ",");
        String argument = "";
        while (tokenizer.hasMoreTokens()) {
            argument = tokenizer.nextToken();
            argument = argument.trim();
            arguments.add(argument);
        }
        return arguments;
    }

    public int findChoice(String argument) {
        if (argument.compareToIgnoreCase("help") == 0)
            return 1;
        else if (argument.compareToIgnoreCase("newflight") == 0)
            return 2;
        else if (argument.compareToIgnoreCase("newcar") == 0)
            return 3;
        else if (argument.compareToIgnoreCase("newroom") == 0)
            return 4;
        else if (argument.compareToIgnoreCase("newcustomer") == 0)
            return 5;
        else if (argument.compareToIgnoreCase("deleteflight") == 0)
            return 6;
        else if (argument.compareToIgnoreCase("deletecar") == 0)
            return 7;
        else if (argument.compareToIgnoreCase("deleteroom") == 0)
            return 8;
        else if (argument.compareToIgnoreCase("deletecustomer") == 0)
            return 9;
        else if (argument.compareToIgnoreCase("queryflight") == 0)
            return 10;
        else if (argument.compareToIgnoreCase("querycar") == 0)
            return 11;
        else if (argument.compareToIgnoreCase("queryroom") == 0)
            return 12;
        else if (argument.compareToIgnoreCase("querycustomer") == 0)
            return 13;
        else if (argument.compareToIgnoreCase("queryflightprice") == 0)
            return 14;
        else if (argument.compareToIgnoreCase("querycarprice") == 0)
            return 15;
        else if (argument.compareToIgnoreCase("queryroomprice") == 0)
            return 16;
        else if (argument.compareToIgnoreCase("reserveflight") == 0)
            return 17;
        else if (argument.compareToIgnoreCase("reservecar") == 0)
            return 18;
        else if (argument.compareToIgnoreCase("reserveroom") == 0)
            return 19;
        else if (argument.compareToIgnoreCase("itinerary") == 0)
            return 20;
        else if (argument.compareToIgnoreCase("quit") == 0)
            return 21;
        else if (argument.compareToIgnoreCase("newcustomerid") == 0)
            return 22;
        else if (argument.compareToIgnoreCase("start") == 0)
            return 23;
        else if (argument.compareToIgnoreCase("abort") == 0)
            return 24;
        else if (argument.compareToIgnoreCase("commit") == 0)
            return 25;
        else if (argument.compareToIgnoreCase("shutdown") == 0)
            return 66;
        else
            return 666;
    }

    public void listCommands() {
        System.out.println("\nWelcome to the client interface provided to test your project.");
        System.out.println("Commands accepted by the interface are: ");
        System.out.println("help");
        System.out.println("newflight\nnewcar\nnewroom\nnewcustomer\nnewcustomerid\ndeleteflight\ndeletecar\ndeleteroom");
        System.out.println("deletecustomer\nqueryflight\nquerycar\nqueryroom\nquerycustomer");
        System.out.println("queryflightprice\nquerycarprice\nqueryroomprice");
        System.out.println("reserveflight\nreservecar\nreserveroom\nitinerary");
        System.out.println("quit");
        System.out.println("\ntype help, <commandname> for detailed info (note the use of comma).");
    }


    public void listSpecific(String command) {
        System.out.print("Help on: ");
        switch (findChoice(command)) {
            case 1:
                System.out.println("Help");
                System.out.println("\nTyping help on the prompt gives a list of all the commands available.");
                System.out.println("Typing help, <commandname> gives details on how to use the particular command.");
                break;

            case 2:  //new flight
                System.out.println("Adding a new ResourceManager.Flight.");
                System.out.println("Purpose: ");
                System.out.println("\tAdd information about a new flight.");
                System.out.println("\nUsage: ");
                System.out.println("\tnewflight, <id>, <flightnumber>, <numSeats>, <flightprice>");
                break;

            case 3:  //new car
                System.out.println("Adding a new car.");
                System.out.println("Purpose: ");
                System.out.println("\tAdd information about a new car location.");
                System.out.println("\nUsage: ");
                System.out.println("\tnewcar, <id>, <location>, <numberofcars>, <pricepercar>");
                break;

            case 4:  //new room
                System.out.println("Adding a new room.");
                System.out.println("Purpose: ");
                System.out.println("\tAdd information about a new room location.");
                System.out.println("\nUsage: ");
                System.out.println("\tnewroom, <id>, <location>, <numberofrooms>, <priceperroom>");
                break;

            case 5:  //new ResourceManager.Customer
                System.out.println("Adding a new ResourceManager.Customer.");
                System.out.println("Purpose: ");
                System.out.println("\tGet the system to provide a new customer id. (same as adding a new customer)");
                System.out.println("\nUsage: ");
                System.out.println("\tnewcustomer, <id>");
                break;


            case 6: //delete ResourceManager.Flight
                System.out.println("Deleting a flight");
                System.out.println("Purpose: ");
                System.out.println("\tDelete a flight's information.");
                System.out.println("\nUsage: ");
                System.out.println("\tdeleteflight, <id>, <flightnumber>");
                break;

            case 7: //delete car
                System.out.println("Deleting a car");
                System.out.println("Purpose: ");
                System.out.println("\tDelete all cars from a location.");
                System.out.println("\nUsage: ");
                System.out.println("\tdeletecar, <id>, <location>, <numCars>");
                break;

            case 8: //delete room
                System.out.println("Deleting a room");
                System.out.println("\nPurpose: ");
                System.out.println("\tDelete all rooms from a location.");
                System.out.println("Usage: ");
                System.out.println("\tdeleteroom, <id>, <location>, <numRooms>");
                break;

            case 9: //delete ResourceManager.Customer
                System.out.println("Deleting a ResourceManager.Customer");
                System.out.println("Purpose: ");
                System.out.println("\tRemove a customer from the database.");
                System.out.println("\nUsage: ");
                System.out.println("\tdeletecustomer, <id>, <customerid>");
                break;

            case 10: //querying a flight
                System.out.println("Querying flight.");
                System.out.println("Purpose: ");
                System.out.println("\tObtain Seat information about a certain flight.");
                System.out.println("\nUsage: ");
                System.out.println("\tqueryflight, <id>, <flightnumber>");
                break;

            case 11: //querying a car Location
                System.out.println("Querying a car location.");
                System.out.println("Purpose: ");
                System.out.println("\tObtain number of cars at a certain car location.");
                System.out.println("\nUsage: ");
                System.out.println("\tquerycar, <id>, <location>");
                break;

            case 12: //querying a room location
                System.out.println("Querying a room Location.");
                System.out.println("Purpose: ");
                System.out.println("\tObtain number of rooms at a certain room location.");
                System.out.println("\nUsage: ");
                System.out.println("\tqueryroom, <id>, <location>");
                break;

            case 13: //querying ResourceManager.Customer Information
                System.out.println("Querying ResourceManager.Customer Information.");
                System.out.println("Purpose: ");
                System.out.println("\tObtain information about a customer.");
                System.out.println("\nUsage: ");
                System.out.println("\tquerycustomer, <id>, <customerid>");
                break;

            case 14: //querying a flight for price 
                System.out.println("Querying flight.");
                System.out.println("Purpose: ");
                System.out.println("\tObtain price information about a certain flight.");
                System.out.println("\nUsage: ");
                System.out.println("\tqueryflightprice, <id>, <flightnumber>");
                break;

            case 15: //querying a car Location for price
                System.out.println("Querying a car location.");
                System.out.println("Purpose: ");
                System.out.println("\tObtain price information about a certain car location.");
                System.out.println("\nUsage: ");
                System.out.println("\tquerycarprice, <id>, <location>");
                break;

            case 16: //querying a room location for price
                System.out.println("Querying a room Location.");
                System.out.println("Purpose: ");
                System.out.println("\tObtain price information about a certain room location.");
                System.out.println("\nUsage: ");
                System.out.println("\tqueryroomprice, <id>, <location>");
                break;

            case 17:  //reserve a flight
                System.out.println("Reserving a flight.");
                System.out.println("Purpose: ");
                System.out.println("\tReserve a flight for a customer.");
                System.out.println("\nUsage: ");
                System.out.println("\treserveflight, <id>, <customerid>, <flightnumber>");
                break;

            case 18:  //reserve a car
                System.out.println("Reserving a car.");
                System.out.println("Purpose: ");
                System.out.println("\tReserve a given number of cars for a customer at a particular location.");
                System.out.println("\nUsage: ");
                System.out.println("\treservecar, <id>, <customerid>, <location>, <nummberofcars>");
                break;

            case 19:  //reserve a room
                System.out.println("Reserving a room.");
                System.out.println("Purpose: ");
                System.out.println("\tReserve a given number of rooms for a customer at a particular location.");
                System.out.println("\nUsage: ");
                System.out.println("\treserveroom, <id>, <customerid>, <location>, <nummberofrooms>");
                break;

            case 20:  //reserve an Itinerary
                System.out.println("Reserving an Itinerary.");
                System.out.println("Purpose: ");
                System.out.println("\tBook one or more flights.Also book zero or more cars/rooms at a location.");
                System.out.println("\nUsage: ");
                System.out.println("\titinerary, <id>, <customerid>, "
                        + "<flightnumber1>....<flightnumberN>, "
                        + "<LocationToBookcarsOrrooms>, <NumberOfcars>, <NumberOfroom>");
                break;


            case 21:  //quit the client
                System.out.println("Quitting client.");
                System.out.println("Purpose: ");
                System.out.println("\tExit the client application.");
                System.out.println("\nUsage: ");
                System.out.println("\tquit");
                break;

            case 22:  //new customer with id
                System.out.println("Create new customer providing an id");
                System.out.println("Purpose: ");
                System.out.println("\tCreates a new customer with the id provided");
                System.out.println("\nUsage: ");
                System.out.println("\tnewcustomerid, <id>, <customerid>");
                break;

            default:
                System.out.println(command);
                System.out.println("The interface does not support this command.");
                break;
        }
    }

    public void wrongNumber() {
        System.out.println("The number of arguments provided in this command are wrong.");
        System.out.println("Type help, <commandname> to check usage of this command.");
    }

    public int getInt(Object temp) throws Exception {
        try {
            return (new Integer((String) temp)).intValue();
        } catch (Exception e) {
            throw e;
        }
    }

    public boolean getBoolean(Object temp) throws Exception {
        try {
            return (new Boolean((String) temp)).booleanValue();
        } catch (Exception e) {
            throw e;
        }
    }

    public String getString(Object temp) throws Exception {
        try {
            return (String) temp;
        } catch (Exception e) {
            throw e;
        }
    }

}
