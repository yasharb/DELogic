/*
 * All Rights Reserved
 */
package dataeaglealpha;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

/**
 *
 * @author Gavin Jenkins To do:
 *
 * Make more java friendly versions of some SQL functions, like just a thing
 * that just inputs variable name and has a method like "getNext", but
 * internally, it buffers up 10,000 lines at a time in a transaction and holds
 * it ready to dispense out with the next getNext. While also handling the
 * annoying syntax and string manipulation. So then in java code elsewhere I can
 * just do a for loop and call DBUtility.getNext("variable") otherwise acting
 * just as if it were an array or something.
 *
 * fix indices
 */
public class DBUtility {

    private static Connection c;
    private static Statement stmt;

    public static void DBUtilityInit(String fileCSV, String fileDB) {
        try {
            c = DriverManager.getConnection(fileDB);
            stmt = c.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Integer[][] getUser(Integer user) {
        try {
            ResultSet queryResult = stmt.executeQuery("SELECT * FROM T WHERE user == '??'");
            System.out.println(queryResult.getInt(0));
            System.out.println(queryResult.getInt(1));
            System.out.println(queryResult.getInt(2));
            System.out.println(queryResult.getInt(3));
            queryResult.next();
            System.out.println(queryResult.getInt(0));
            System.out.println(queryResult.getInt(1));
            System.out.println(queryResult.getInt(2));
            System.out.println(queryResult.getInt(3));
            queryResult.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //Integer[][] answer = new Integer[queryResult.][];
        Integer[][] test = new Integer[1][1];
        test[0][0] = 5;
        return test;
        
    }

    public static void queueField(String field) {
        //designed to be a method for "start buffering blah blah field in java so I can start harvesting lines for basic stats and such intuitively"
        //but not sure how relevant anymore.
    }

    public static int getNextInt(String fileCSV, String fileDB) {
        int answer = 0;
        //have buffering transactions built in here somewhere, maybe global to the class? Hm. maybe something more different than this
        //but want a means of just asking for the next line similr to a filereader in java, without doing all the transaction and string juggling etc. elsewhere.
        return answer;
    }

    public static void csvToDatabase(String fileCSV, String fileDB) {
        BufferedReader br = null;
        String line;
        String delim = ",";
        String[] varsString;
        Integer[] varsInt = new Integer[4];
        String sqlAppend;
        String sql;
        String date = "";
        Boolean done = false;
        Boolean newDB = true; //manual entry parameter for now, testing
        int linesAtOnce = 100000;
        HashMap<String, Integer> stringToID = new HashMap<>();
        HashMap<Integer, String> IDToString = new HashMap<>();
        HashMap<String, Integer> stringToEvent = new HashMap<>();
        HashMap<Integer, String> eventToString = new HashMap<>();

        DBUtilityInit(fileCSV, fileDB);

        try {
            br = new BufferedReader(new FileReader(fileCSV));
            line = br.readLine();
            System.out.println(line);
            if (newDB) { //Will have standardized format, so just hardcoded... all are INTs because I convet unique values to integer codes first for sanity.
                sql = "CREATE TABLE IF NOT EXISTS T(user INT, time INT, event INT, eventType INT)";
                stmt.executeUpdate(sql);
                //go through and collect all the unique IDs and event names in hashmaps
                int IDCounter = 1;
                int EventCounter = 1;
                while ((line = br.readLine()) != null) {
                    varsString = line.split(delim);
                    if (!stringToID.containsKey(varsString[0])) { //!!!!!! Note: later on when reading in new updated lines, still need to check them to see if unique and update hashes.
                        stringToID.put(varsString[0], IDCounter);
                        IDToString.put(IDCounter, varsString[0]);
                        IDCounter++;
                    }
                    if (!stringToEvent.containsKey(varsString[2])) {
                        stringToEvent.put(varsString[2], EventCounter);
                        eventToString.put(EventCounter, varsString[2]);
                        EventCounter++;
                    }
                }
            } else { //load up existing hashmaps
                try {
                    FileInputStream fis = new FileInputStream("stringToID.ser");
                    ObjectInputStream ois = new ObjectInputStream(fis);
                    stringToID = (HashMap) ois.readObject();
                    fis = new FileInputStream("stringToEvent.ser");
                    ois = new ObjectInputStream(fis);
                    stringToEvent = (HashMap) ois.readObject();
                    ois.close();
                    fis = new FileInputStream("IDToString.ser");
                    ois = new ObjectInputStream(fis);
                    IDToString = (HashMap) ois.readObject();
                    ois.close();
                    fis = new FileInputStream("eventToString.ser");
                    ois = new ObjectInputStream(fis);
                    eventToString = (HashMap) ois.readObject();
                    ois.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //###########################add something that finds out what line new content begins at (larger time value than in database)
                //!!!!!!!!!!!!!!!!!!!!!!!!!!!and then use that below for all the br.readlines to skip to, so that we can use this to upate too.
                //then add in that bit commented below where you look once again for unique hash values in the new portions of the csv.
                //add indexing again.
            }

            //Now read everything into database
            int j = 0; //counter for output to terminal
            while (done == false) {
                stmt.executeUpdate("BEGIN TRANSACTION"); //start doing batches of line entries, in blocks of linesAtOnce size lined up.
                for (int k = 0; k < linesAtOnce; k++) {
                    if ((line = br.readLine()) != null) { //boring text parsing for lines:
                        j++;
                        sqlAppend = "";
                        varsString = line.split(delim);
                        varsInt[0] = stringToID.get(varsString[0]);
                        varsInt[1] = Integer.parseInt(varsString[1]);
                        varsInt[2] = stringToEvent.get(varsString[2]);
                        if (varsString[3].equals("ui")) {
                            varsInt[3] = 1;
                        } else if (varsString[3].equals("system")) {
                            varsInt[3] = 2;
                        } else if (varsString[3].equals("transition")) {
                            varsInt[3] = 3;
                        } else {
                            System.out.println("Invalid/Unknown event type encountered!");
                        }
                        for (int i = 0; i < (varsInt.length); i++) {
                            if (i != varsInt.length - 1) {
                                sqlAppend = sqlAppend + " " + Integer.toString(varsInt[i]) + ",";
                            } else {
                                sqlAppend = sqlAppend + " " + Integer.toString(varsInt[i]);
                            }
                        }
                        sql = "INSERT INTO T VALUES(" + sqlAppend + ")";
                        stmt.executeUpdate(sql);
                    } else {
                        done = true;
                    }
                }
                stmt.executeUpdate("COMMIT");
                System.out.println(j);
            }
            stmt.executeUpdate("CREATE INDEX user ON T (user)");
            stmt.executeUpdate("CREATE INDEX event ON T (event)");
            stmt.close();
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        //Now save the hashmaps to disk
        try {
            FileOutputStream fos = new FileOutputStream("stringToID.ser");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(stringToID);
            oos.close();
            fos = new FileOutputStream("stringToEvent.ser");
            oos = new ObjectOutputStream(fos);
            oos.writeObject(stringToEvent);
            oos.close();
            fos = new FileOutputStream("IDToString.ser");
            oos = new ObjectOutputStream(fos);
            oos.writeObject(IDToString);
            oos.close();
            fos = new FileOutputStream("eventToString.ser");
            oos = new ObjectOutputStream(fos);
            oos.writeObject(eventToString);
            oos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //"Connect" to a database, weird stuff that is acting like it's on a server farm:
    public static void createNewDatabase(String fileName, String dir) {

        String url = dir + fileName;

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
