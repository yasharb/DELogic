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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.Stream;

enum LoadType {
    LoadTypeOnlyLoad,
    LoadTypeAppend,
    LoadTypeNew
}

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
    private static Integer maxUserId;
    private static Integer maxEventId;
    
    private static int linesAtOnce = 100000;
    private static HashMap<String, Integer> stringToID = new HashMap<>();
    private static HashMap<Integer, String> IDToString = new HashMap<>();
    private static HashMap<String, Integer> stringToEvent = new HashMap<>();
    private static HashMap<Integer, String> eventToString = new HashMap<>();

    public static void setStringToID(HashMap<String, Integer> stringToID) {
        DBUtility.stringToID = stringToID;
    }

    public static void setIDToString(HashMap<Integer, String> IDToString) {
        DBUtility.IDToString = IDToString;
    }

    public static void setStringToEvent(HashMap<String, Integer> stringToEvent) {
        DBUtility.stringToEvent = stringToEvent;
    }

    public static void setEventToString(HashMap<Integer, String> eventToString) {
        DBUtility.eventToString = eventToString;
    }
    
    private static HashMap<String, Integer> csvFileLinesRead = new HashMap<>();
    
    public static void DBUtilityInit(String fileCSV, String fileDB) {
        
        maxUserId = 0;
        maxEventId = 0;
        
        try {
            c = DriverManager.getConnection(fileDB);
            stmt = c.createStatement();
            String sql = "CREATE TABLE IF NOT EXISTS T(user INT, time INT, event INT, eventType INT)";
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    
    
    /**
     * Gives the total number of users in DB
     * @return total users
     */
    public static Integer getMaxUserId() {
        return maxUserId;
    }
    
    /**
     * Gives the total number of different events.
     * @return total events
     */
    public static Integer getMaxEventId() {
        return maxEventId;
    }
    
    /**
     * Gives all the events related to a specific user.
     * @param user The integer user id for a specific user. 
     * @return Returns a two dimensional array of events, the first dimension being the row and second the column. Columns are in order userId, time, eventId, eventType.
     */
    public static Integer[][] getEventsForUser(Integer user) {
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

    public static void appendCSVToDatabase(String fileCSV, String fileDB) {
        csvToDatabase(fileCSV, fileDB, LoadType.LoadTypeAppend);
    }
    
    public static void setupDatabaseForNewCSV(String fileCSV, String fileDB) {
        csvToDatabase(fileCSV, fileDB, LoadType.LoadTypeNew);
    }
    
    public static void loadDatabase(String fileDB) {
        csvToDatabase("", fileDB, LoadType.LoadTypeOnlyLoad);
    }
    
    private static void loadCSVAndUpdateHashmaps(String fileCSV) throws FileNotFoundException, IOException {
        
        // the first line is the header line so we skip it.
        loadCSVAndUpdateHashmaps(fileCSV, 1);
        
    }
    
    private static void loadCSVAndUpdateHashmaps (String fileCSV, Integer startingLine) throws FileNotFoundException, IOException {
        
        String delim = ",";
        String[] varsString;
        
        Boolean newUserSeen = false;
        Boolean newEventSeen = false;
        
        int lineCounter = startingLine;
        
        try (Stream<String> lines = Files.lines(Paths.get(fileCSV))) {
            Stream<String> newLines = lines.skip(startingLine);
            
            for (Iterator<String> iterator = newLines.iterator(); iterator.hasNext();) {
                String line = iterator.next();
                lineCounter++;
                
                varsString = line.split(delim);
                if (!stringToID.containsKey(varsString[0])) { //!!!!!! Note: later on when reading in new updated lines, still need to check them to see if unique and update hashes.
                    stringToID.put(varsString[0], maxUserId);
                    IDToString.put(maxUserId, varsString[0]);
                    maxUserId++;
                    newUserSeen = true;
                }
                if (!stringToEvent.containsKey(varsString[2])) {
                    stringToEvent.put(varsString[2], maxEventId);
                    eventToString.put(maxEventId, varsString[2]);
                    maxEventId++;
                    newEventSeen = true;
                }
                
            }
            
            // if we've needlessly incremented the max ints decrement once at the end.
            if (newUserSeen) maxUserId--;
            if (newEventSeen) maxEventId--;
            
            // store what line we're at in the loaded file.
            csvFileLinesRead.put(fileCSV, lineCounter);
            
        }
        
    }
    
    private static void loadExistingHashmaps() throws FileNotFoundException, IOException, ClassNotFoundException {
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
        fis = new FileInputStream("csvFileLinesRead.ser");
        ois = new ObjectInputStream(fis);
        csvFileLinesRead = (HashMap) ois.readObject();
        ois.close();

        // we want to measure max values;
        Integer i;
        for (Iterator iterator = IDToString.keySet().iterator(); iterator.hasNext();) {
            i = (Integer) iterator.next();
            if (i > maxUserId) {
                maxUserId = i;
            }
        }

        for (Iterator iterator = eventToString.keySet().iterator(); iterator.hasNext();) {
            i = (Integer) iterator.next();
            if (i > maxEventId) {
                maxEventId = i;
            }
        }
    }
    
    /**
     * Loads given CSV file into database while also setting up the hash tables,
     * measuring maximum events and users and any other initial DB setup stuff.
     * @param fileCSV The file address for the CSV file.
     * @param fileDB The DB URL. For example 'jdbc:sqlite:./sibche.db'
     * @param loadMode  0 is for loading existing things.
     *                  1 is for creating a new DB and loading fresh CSV data
     *                  2 is for appending CSV data to existing database.
     */
    public static void csvToDatabase(String fileCSV, String fileDB, LoadType loadMode) {
        
        // setup the DB file.
        DBUtilityInit(fileCSV, fileDB);
        
        BufferedReader br = null;
        String line;
        String delim = ",";
        String[] varsString;
        Integer[] varsInt = new Integer[4];
        String sqlAppend;
        String sql;
        String date = "";
        Boolean done = false;
              
        try {
            if (loadMode == LoadType.LoadTypeNew) { //Will have standardized format, so just hardcoded... all are INTs because I convet unique values to integer codes first for sanity.
                // if we have a new file or are appending new 
                
                loadCSVAndUpdateHashmaps(fileCSV);
                
            } else { //load up existing hashmaps
                
                loadExistingHashmaps();
                
                //###########################add something that finds out what line new content begins at (larger time value than in database)
                //!!!!!!!!!!!!!!!!!!!!!!!!!!!and then use that below for all the br.readlines to skip to, so that we can use this to upate too.
                //then add in that bit commented below where you look once again for unique hash values in the new portions of the csv.
                //add indexing again.
                if (loadMode == LoadType.LoadTypeAppend) {
                    // if we're appending to existing data then we want to load 
                    //  our CSV file from a certain line.
                    
                    if (csvFileLinesRead.containsKey(fileCSV)) {
                        Integer lineNumber = csvFileLinesRead.get(fileCSV);
                        loadCSVAndUpdateHashmaps(fileCSV, lineNumber);
                    }else {
                        // if we've never seen the file before then go ahead read it from the start.
                        loadCSVAndUpdateHashmaps(fileCSV);
                    }  
                }
            }
                
            //Now read everything into database
            br = new BufferedReader(new FileReader(fileCSV));
            line = br.readLine(); // we read the first line since it's the column names.
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
            saveHashmapsToDisk();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void saveHashmapsToDisk() throws FileNotFoundException, IOException {
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
        fos = new FileOutputStream("csvFileLinesRead.ser");
        oos = new ObjectOutputStream(fos);
        oos.writeObject(csvFileLinesRead);
        oos.close();
    }

}
