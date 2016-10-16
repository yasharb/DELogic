/*
 * All Rights Reserved
 */
package dataeaglealpha;

/**
 * Performs (or loads from earlier) basic summary statistics for whole database, commonly used elsewhere
 * Each variable:
 * N
 * standard deviation
 * mean
 * median
 * range
 * skewness
 *
 * @author Gavin
 */
public class SummaryStats {
    
    private int[][] statsArray;
    //0: N
    //1: Standard Deviation
    //2: Mean
    //3: Median
    //4: Range
    //5: Skewness

    public void SummaryStates() { //for loading, or for later when writing individual stats test methodds to do separately if desired
    }

    public SummaryStats(String db) {
        Analyze(db);
    }
    
    public static void Analyze(String db){
        //find all variables in db
        //buffer up transactions in large amounts, maybe optional param with a default
        //do all the things needed to keep temporary tallies and sum of squares blah blah
        //when done, finalize in statsArray, and call save function.
    }
    
    //save method to file -- be sure to check one doesn't exist, if it does, version, if old only add on new stuff if a speed problem, etc.
    
    //load method to be able to make this object and load up all the things
    
    //various getter methods for other classe to grab the summary stats of variables from statsArray
    
    //methods for newly added tests later, and possibly also ones that update old file?
}
