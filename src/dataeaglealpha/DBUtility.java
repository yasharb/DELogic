/*
 * All Rights Reserved
 */
package dataeaglealpha;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
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
import java.util.Vector;

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
    
    private static HashMap<Integer, String> eventTypeToString = new HashMap<>();
    private static HashMap<String, Integer> stringToEventType = new HashMap<>();

    public static HashMap<Integer, String> getEventTypeToString() {
        return eventTypeToString;
    }

    public static HashMap<String, Integer> getStringToEventType() {
        return stringToEventType;
    }

    public static HashMap<String, Integer> getStringToID() {
        return stringToID;
    }

    public static HashMap<Integer, String> getIDToString() {
        return IDToString;
    }

    public static HashMap<String, Integer> getStringToEvent() {
        return stringToEvent;
    }

    public static HashMap<Integer, String> getEventToString() {
        return eventToString;
    }
    
    private static HashMap<String, Integer> csvFileLinesRead = new HashMap<>();
    
    public static void DBUtilityInit(String fileCSV, String fileDB, LoadType loadMode) {
        
        maxUserId = 0;
        maxEventId = 0;
        
        try {
            
            c = DriverManager.getConnection(fileDB);
            stmt = c.createStatement();
            
            if (loadMode == LoadType.LoadTypeNew) {
                // delete old table.
                stmt.executeUpdate("DROP TABLE IF EXISTS T");
            }
            
            String sql = "CREATE TABLE IF NOT EXISTS T(user INT, time INT, event INT, eventType INT)";
            stmt.executeUpdate(sql);
            
            if (loadMode == LoadType.LoadTypeNew) {
                // add indices 
                stmt.executeUpdate("CREATE INDEX user ON T (user)");
                stmt.executeUpdate("CREATE INDEX event ON T (event)"); 
            }
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
    public static Vector<Vector<Integer>> getEventsForUser(Integer user) {
        Vector<Vector<Integer>> dataBlock = new Vector<Vector<Integer>>();
        Vector<Integer> row = new Vector<Integer>();
        try {
            ResultSet queryResult = stmt.executeQuery("SELECT * FROM T WHERE user = " + user);
            row = new Vector<Integer>();
            while (queryResult.next()==true){
                row.add(queryResult.getInt(1));
                row.add(queryResult.getInt(2));
                row.add(queryResult.getInt(3));
                row.add(queryResult.getInt(4));
                dataBlock.add(row);
            }
            queryResult.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dataBlock;
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
        int eventTypeCounter = 0;
        
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
                if (!stringToEventType.containsKey(varsString[3])) {
                    stringToEventType.put(varsString[3], eventTypeCounter);
                    eventTypeToString.put(eventTypeCounter, varsString[3]);
                    eventTypeCounter++;
                    
                }
                
            }
            
            // if we've needlessly incremented the max ints decrement once at the end.
            if (newUserSeen) maxUserId--;
            if (newEventSeen) maxEventId--;
            
            // store what line we're at in the loaded file.
            csvFileLinesRead.put(fileCSV, lineCounter);
            
            newLines.close();
        }
        
    }
    
    private static HashMap loadMapFile(HashMap map, String fileName) throws FileNotFoundException, IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(fileName);
        ObjectInputStream ois = new ObjectInputStream(fis);
        map = (HashMap) ois.readObject();
        ois.close();
        fis.close();
        return map;
    }
    
    private static void loadExistingHashmaps() throws FileNotFoundException, IOException, ClassNotFoundException {
        stringToID = loadMapFile(stringToID, "stringToID.ser");
        IDToString = loadMapFile(IDToString, "IDToString.ser");
        stringToEvent = loadMapFile(stringToEvent, "stringToEvent.ser");
        eventToString = loadMapFile(eventToString, "eventToString.ser");
        csvFileLinesRead = loadMapFile(csvFileLinesRead, "csvFileLinesRead.ser");
        stringToEventType = loadMapFile(stringToEventType, "stringToEventType.ser");
        eventTypeToString = loadMapFile(eventTypeToString, "eventTypeToString.ser");
        
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
        
        System.out.println(maxEventId);
    }
    
    private static void addEventsToDB(String fileCSV, Integer startingLine) throws FileNotFoundException, IOException, SQLException {
        
        String line;
        String delim = ",";
        String[] varsString;
        Integer[] varsInt = new Integer[4];
        String sqlAppend;
        String sql;
        String date = "";
        Boolean done = false;
        int j = 0; //counter for output to terminal
        int lineCounter = 0;
        try (Stream<String> lines = Files.lines(Paths.get(fileCSV))) {
            Stream<String> newLines = lines.skip(startingLine);
            
            stmt.executeUpdate("BEGIN TRANSACTION");
            
            for (Iterator<String> iterator = newLines.iterator(); iterator.hasNext();) {
                line = iterator.next();
                
                sqlAppend = "";
                varsString = line.split(delim);
                varsInt[0] = stringToID.get(varsString[0]);
                varsInt[1] = Integer.parseInt(varsString[1]);
                varsInt[2] = stringToEvent.get(varsString[2]);
                varsInt[3] = stringToEventType.get(varsString[3]);

                for (int i = 0; i < (varsInt.length); i++) {
                    if (i != varsInt.length - 1) {
                        sqlAppend = sqlAppend + " " + Integer.toString(varsInt[i]) + ",";
                    } else {
                        sqlAppend = sqlAppend + " " + Integer.toString(varsInt[i]);
                    }
                }
                sql = "INSERT INTO T VALUES(" + sqlAppend + ")";
                stmt.executeUpdate(sql);
                
                j++;
                lineCounter++;
                // check to see if we've reached commit limit and if so do a commit.
                if (lineCounter > linesAtOnce) {
                    // do commit.
                    stmt.executeUpdate("COMMIT");
                    stmt.executeUpdate("BEGIN TRANSACTION");
                }
            }
            
            stmt.executeUpdate("COMMIT");
            newLines.close();
        }
        
        System.out.println(j);
       
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
        DBUtilityInit(fileCSV, fileDB, loadMode);
        
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
                addEventsToDB(fileCSV, 1);
                
            } else { //load up existing hashmaps
                
                loadExistingHashmaps();
                
                if (loadMode == LoadType.LoadTypeAppend) {
                    // if we're appending to existing data then we want to load 
                    //  our CSV file from a certain line.
                    
                    if (csvFileLinesRead.containsKey(fileCSV)) {
                        Integer lineNumber = csvFileLinesRead.get(fileCSV);
                        loadCSVAndUpdateHashmaps(fileCSV, lineNumber);
                        addEventsToDB(fileCSV, lineNumber);
                    }else {
                        // if we've never seen the file before then go ahead read it from the start.
                        loadCSVAndUpdateHashmaps(fileCSV);
                        addEventsToDB(fileCSV, 1);
                    }  
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        } 
        //Now save the hashmaps to disk
        try {
            saveHashmapsToDisk();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void saveMapFile(HashMap map, String filename) throws FileNotFoundException, IOException {
        FileOutputStream fos = new FileOutputStream(filename);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(map);
        oos.close();
        fos.close();
    }
    
    private static void saveHashmapsToDisk() throws FileNotFoundException, IOException {
        saveMapFile(stringToID, "stringToID.ser");
        saveMapFile(IDToString, "IDToString.ser");
        saveMapFile(stringToEvent, "stringToEvent.ser");
        saveMapFile(eventToString, "eventToString.ser");
        saveMapFile(csvFileLinesRead, "csvFileLinesRead.ser");
        saveMapFile(stringToEventType, "stringToEventType.ser");
        saveMapFile(eventTypeToString, "eventTypeToString.ser");
    }

}
