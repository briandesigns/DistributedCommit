package Persistance;

import ResourceManager.RMHashtable;
import ResourceManager.TCPServer;

import java.io.*;

/**
 * Created by brian on 20/11/15.
 */

//todo: how does data persistence work exactly?
public class DiskOperator {
    public void writeDataToDisk(RMHashtable table, String fileName) throws IOException {
        String path = getJarDirectoryPath();
        path = path + "/" + fileName + ".ser";
        FileOutputStream toFile = new FileOutputStream(path);
        ObjectOutputStream objectToFile = new ObjectOutputStream(toFile);
        objectToFile.writeObject(table);
        objectToFile.close();
        System.out.println("data writen to file:" + path);
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
}
