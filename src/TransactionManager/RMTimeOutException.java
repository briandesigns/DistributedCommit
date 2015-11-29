package TransactionManager;

/**
 * Created by brian on 28/11/15.
 */
public class RMTimeOutException extends Exception {
    public RMTimeOutException(String type) {
        super(type + " RM timed out");
    }
}
