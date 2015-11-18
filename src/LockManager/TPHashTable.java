package LockManager;

import java.util.Vector;
import java.util.Enumeration;

/*
    HashTable class for the Lock Manager.
*/

public class TPHashTable {
    private static final int HASH_DEPTH = 8;

    private Vector vect;
    private int iSize;    // size of the hash table

    /**
     * creates a vector(size: iSize) of vectors(size 8)
     * @param iSize
     */
    TPHashTable(int iSize) {
        this.iSize = iSize;

        vect = new Vector(iSize);
        for (int i = 0; i < iSize; i++) {
            this.vect.addElement(new Vector(this.HASH_DEPTH));
        }
    }

    public int getSize() {
        return iSize;
    }

    /**
     * append the Xobj to the vector in vectorslot of Vect corresponding to the Xobj's Xid
     * @param xobj xobj to be added to the hashtable
     */
    public synchronized void add(XObj xobj) {
        if (xobj == null) return;

        Vector vectSlot;

        int hashSlot = (xobj.hashCode() % this.iSize);
        if (hashSlot < 0) {
            hashSlot = -hashSlot;
        }
        vectSlot = (Vector) vect.elementAt(hashSlot);
        vectSlot.addElement(xobj);
    }

    /**
     *
     * @param xobj
     * @return
     */
    public synchronized Vector elements(XObj xobj) {
        if (xobj == null) return (new Vector());

        Vector vectSlot;    // hash slot
        Vector elemVect = new Vector(24); // return object

        int hashSlot = (xobj.hashCode() % this.iSize);
        if (hashSlot < 0) {
            hashSlot = -hashSlot;
        }

        vectSlot = (Vector) vect.elementAt(hashSlot);

        XObj xobj2;
        int size = vectSlot.size();
        for (int i = 0; i < size; i++) {
            xobj2 = (XObj) vectSlot.elementAt(i);
            if (xobj.key() == xobj2.key()) {
                elemVect.addElement(xobj2);
            }
        }
        return elemVect;
    }

    /**
     * check if vect contain xObj based on Vector.contains() which checks objectID and not XId
     * @param xobj
     * @return
     */
    public synchronized boolean contains(XObj xobj) {
        if (xobj == null) return false;

        Vector vectSlot;

        int hashSlot = (xobj.hashCode() % this.iSize);
        if (hashSlot < 0) {
            hashSlot = -hashSlot;
        }

        vectSlot = (Vector) vect.elementAt(hashSlot);
        return vectSlot.contains(xobj);
    }

    /**
     * remove xobj based on Object ID
     * @param xobj
     * @return
     */
    //todo: remove based on Xid only?
    public synchronized boolean remove(XObj xobj) {
        if (xobj == null) return false;

        Vector vectSlot;

        int hashSlot = (xobj.hashCode() % this.iSize);
        if (hashSlot < 0) {
            hashSlot = -hashSlot;
        }

        vectSlot = (Vector) vect.elementAt(hashSlot);
        return vectSlot.removeElement(xobj);
    }

    /**
     * @param xobj
     * @return the XObj based on Object ID if it exist in Vect, null if it does not
     */
    public synchronized XObj get(XObj xobj) {
        if (xobj == null) return null;

        Vector vectSlot;

        int hashSlot = (xobj.hashCode() % this.iSize);
        if (hashSlot < 0) {
            hashSlot = -hashSlot;
        }

        vectSlot = (Vector) vect.elementAt(hashSlot);

        XObj xobj2;
        int size = vectSlot.size();
        for (int i = 0; i < size; i++) {
            xobj2 = (XObj) vectSlot.elementAt(i);
            if (xobj.equals(xobj2)) {
                return xobj2;
            }
        }
        return null;
    }

    private void printStatus(String msg, int hashSlot, XObj xobj) {
        System.out.println(this.getClass() + "::" + msg + "(slot" + hashSlot + ")::" + xobj.toString());
    }

    public Vector allElements() {
        Vector vectSlot = null;
        XObj xobj = null;
        Vector hashContents = new Vector(1024);

        for (int i = 0; i < this.iSize; i++) {    // walk down hashslots
            if ((this.vect).size() > 0) {    // contains elements?
                vectSlot = (Vector) (this.vect).elementAt(i);

                for (int j = 0; j < vectSlot.size(); j++) {    // walk down single hash slot, adding elements.
                    xobj = (XObj) vectSlot.elementAt(j);
                    hashContents.addElement(xobj);
                }
            }
            // else contributes nothing.
        }

        return hashContents;
    }

    public synchronized void removeAll(XObj xobj) {
        if (xobj == null) return;

        Vector vectSlot;

        int hashSlot = (xobj.hashCode() % this.iSize);
        if (hashSlot < 0) {
            hashSlot = -hashSlot;
        }

        vectSlot = (Vector) vect.elementAt(hashSlot);

        XObj xobj2;
        int size = vectSlot.size();
        for (int i = (size - 1); i >= 0; i--) {
            xobj2 = (XObj) vectSlot.elementAt(i);
            if (xobj.key() == xobj2.key()) {
                vectSlot.removeElementAt(i);
            }
        }
    }
}
