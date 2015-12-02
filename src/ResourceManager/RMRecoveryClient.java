package ResourceManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Created by brian on 01/12/15.
 */
public class RMRecoveryClient {
    private String mwHost;
    private int mwPort;
    private Socket mwSocket;
    protected PrintWriter toMW;
    protected BufferedReader fromMW;


    public RMRecoveryClient(String mwHost, int mwPort) throws IOException {
        this.mwHost = mwHost;
        this.mwPort = mwPort;
        mwSocket = new Socket(mwHost, mwPort);
        toMW = new PrintWriter(mwSocket.getOutputStream(), true);
        fromMW = new BufferedReader(new InputStreamReader(mwSocket.getInputStream()));
    }


    public String getShadowCopy() {

        String shadowCopy;
        toMW.println("master");

        try {
            shadowCopy = fromMW.readLine();
            Trace.info("shadow copy obtained form mw:" + shadowCopy);
        } catch (IOException e) {
            Trace.error("cannot communicate with mw for recovery purposees");
            shadowCopy = "";
        }
        return shadowCopy;
    }

    public void disconnect() {
        toMW.close();
        try {
            fromMW.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
