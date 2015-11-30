package Persistance;

import ResourceManager.RMHashtable;
import ResourceManager.TCPServer;
import ResourceManager.Trace;

import java.io.*;

/**
 * Created by brian on 20/11/15.
 */

public class DiskOperator {
    public void writeDataToDisk(RMHashtable table, String fileName) throws IOException {
        String path = getJarDirectoryPath();
        path = path + "/" + fileName + ".ser";
        FileOutputStream toFile = new FileOutputStream(path);
        ObjectOutputStream objectToFile = new ObjectOutputStream(toFile);
        objectToFile.writeObject(table);
        objectToFile.close();
        System.out.println("data written to file:" + path);
    }

    public Object getDataFromDisk(String fileName) throws IOException, ClassNotFoundException {
        String path = getJarDirectoryPath();
        path = path + "/" + fileName + ".ser";
        FileInputStream fromFile = new FileInputStream(path);
        ObjectInputStream objectFromFile = new ObjectInputStream(fromFile);
        Object returnedObject = objectFromFile.readObject();
        objectFromFile.close();
        return returnedObject;
    }

    public String getJarDirectoryPath() {
        String path = TCPServer.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        path = new File(path).getParent();
        return path;
    }

    public String readMasterRecord() {
        String path = getJarDirectoryPath();
        path = path + "/master.log";
        File masterRecord = new File(path);
        if (!masterRecord.exists()) {
            try {
                masterRecord.createNewFile();
                return "";
            } catch (IOException e) {
                e.printStackTrace();
                return "";
            }
        } else {

            FileInputStream fis = null;
            try {
                fis = new FileInputStream(masterRecord);
                byte[] data = new byte[(int) masterRecord.length()];
                fis.read(data);
                fis.close();
                String str = new String(data, "UTF-8");
                return str;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return "";
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return "";
            } catch (IOException e) {
                e.printStackTrace();
                return "";
            }
        }
    }

    public boolean writeMasterRecord(String record) {
        String path = getJarDirectoryPath();
        path = path + "/master.log";
        File masterRecord = new File(path);
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(masterRecord);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Trace.error("could not writeMasterRecord");
            return false;
        }
        writer.println(record);
        writer.close();
        return true;
    }

    public boolean clearLogRecord() {
        String path = getJarDirectoryPath();
        if (TCPServer.serverType.equals(TCPServer.MIDDLEWARE)) {
            path = path + "/cord.log";
        } else if (TCPServer.serverType.equals(TCPServer.FLIGHT_RM)) {
            path = path + "/flight_part.log";
        } else if (TCPServer.serverType.equals(TCPServer.CAR_RM)) {
            path = path + "/car_part.log";
        } else if (TCPServer.serverType.equals(TCPServer.ROOM_RM)) {
            path = path + "/room_part.log";
        }
        File masterRecord = new File(path);
        masterRecord.delete();
        try {
            masterRecord.createNewFile();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            Trace.error("failed to clear logRecord");
            return false;
        }
    }

    public boolean writeLogRecord(String record) {
        String path = getJarDirectoryPath();
        if (TCPServer.serverType.equals(TCPServer.MIDDLEWARE)) {
            path = path + "/cord.log";
        } else if (TCPServer.serverType.equals(TCPServer.FLIGHT_RM)) {
            path = path + "/flight_part.log";
        } else if (TCPServer.serverType.equals(TCPServer.CAR_RM)) {
            path = path + "/car_part.log";
        } else if (TCPServer.serverType.equals(TCPServer.ROOM_RM)) {
            path = path + "/room_part.log";
        }
        File masterRecord = new File(path);
        FileWriter writer = null;
        BufferedWriter bw = null;
        try {
            writer = new FileWriter(masterRecord, true);
            bw = new BufferedWriter(writer);
            bw.write(record);
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
            Trace.error("could not write log to disk");
            return false;
        }
        return true;
    }

    public String readLogRecord() {
        String path = getJarDirectoryPath();
        if (TCPServer.serverType.equals(TCPServer.MIDDLEWARE)) {
            path = path + "/cord.log";
        } else if (TCPServer.serverType.equals(TCPServer.FLIGHT_RM)) {
            path = path + "/flight_part.log";
        } else if (TCPServer.serverType.equals(TCPServer.CAR_RM)) {
            path = path + "/car_part.log";
        } else if (TCPServer.serverType.equals(TCPServer.ROOM_RM)) {
            path = path + "/room_part.log";
        }
        File masterRecord = new File(path);
        if (!masterRecord.exists()) {
            try {
                masterRecord.createNewFile();
                return "";
            } catch (IOException e) {
                e.printStackTrace();
                return "";
            }
        } else {

            FileInputStream fis = null;
            try {
                fis = new FileInputStream(masterRecord);
                byte[] data = new byte[(int) masterRecord.length()];
                fis.read(data);
                fis.close();
                String str = new String(data, "UTF-8");
                return str;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return "";
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return "";
            } catch (IOException e) {
                e.printStackTrace();
                return "";
            }
        }
    }
}
