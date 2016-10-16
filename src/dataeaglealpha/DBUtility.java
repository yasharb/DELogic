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

/**
 *
 * @author Gavin
 * To do:
 * 
 * Make more java friendly versions of some SQL functions, like just a thing that just inputs
 * variable name and has a method like "getNext", but internally, it buffers up 10,000 lines at
 * a time in a transaction and holds it ready to dispense out with the next getNext. While also
 * handling the annoying syntax and string manipulation. So then in java code elsewhere I can just do a for loop
 * and call DBUtility.getNext("variable") otherwise acting just as if it were an array or something.
 * 
 * fix indices
 */
public class DBUtility {
    
    

    public static void queueField(String field){
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
        String line = "";
        String delim = "\t";
        String[] vars = new String[21]; // ########## yeah yeah hardcoded, bad Gavin
        String sqlAppend = "";
        String sql;
        String date = "";
        Connection c = null;
        Statement stmt = null;
        Boolean done = false;
        Boolean newDB = true; //manual entry parameter for now, testing
        int linesAtOnce = 100000;

        try {
            System.out.println(fileCSV);
            System.out.println(fileDB);
            c = DriverManager.getConnection(fileDB);
            br = new BufferedReader(new FileReader(fileCSV));
            stmt = c.createStatement();
            line = br.readLine();
            System.out.println(line);
            if (newDB) { //boring text parsing for setting up the create table command:
                vars = line.split(delim);
                sqlAppend = "";
                for (int i = 1; i < (vars.length); i++) {
                    if (i != vars.length - 1) {
                        sqlAppend = sqlAppend + " " + vars[i] + " INT,";
                    } else {
                        sqlAppend = sqlAppend + " " + vars[i] + " INT";
                    }
                }
                sql = "CREATE TABLE IF NOT EXISTS T(" + vars[0] + " " + "CHAR(10)," + sqlAppend + ")";
                stmt.executeUpdate(sql);
            }
            int j = 0;
            while (done == false) {
                stmt.executeUpdate("BEGIN TRANSACTION"); //start doing batches of line entries, in blocks of linesAtOnce size lined up.
                for (int k = 0; k < linesAtOnce; k++) {
                    if ((line = br.readLine()) != null) { //boring text parsing for lines:
                        j++;
                        sqlAppend = "";
                        vars = line.split(delim);
                        for (int i = 0; i < (vars.length); i++) {
                            if (i != vars.length - 1) {
                                sqlAppend = sqlAppend + " " + vars[i] + ",";
                            } else {
                                sqlAppend = sqlAppend + " " + vars[i];
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
            //sql = "CREATE INDEX user on T(ID);"; // hardcoded indices, although in reality we will probably want hardcoded anyway
            //stmt.executeUpdate(sql);
            //sql = "CREATE INDEX date on T(Date);";
            //stmt.executeUpdate(sql);
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
