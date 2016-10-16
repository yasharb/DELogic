/*
 * All Rights Reserved
 */
package dataeaglealpha;

/**
 *
 * @author Gavin
 */
public class PowerAnalysis {

    int[][] powerArray; //vars by stats:
    //0: N
    //1: Standard Deviation
    //2: Mean

    public PowerAnalysis(SummaryStats summaries, String... args) {
        powerArray = new int[args.length][3]; // hardcoded as stats needed for power analysis. Var, N, Mean
        for (String arg : args) {
            //loop through and grab all the answers from summary stats
        }
    }
    
    public PowerAnalysis(String... args) {
        //same as above, but call a new summarystats here and have it analyze first.
    }

    public static void Analyze() {  //separate method not in constructor, may want to try out various options on the same variables.
        //placeholder. Should take as input number of test subjects available
        //and hmm... maybe connections between things. Which vars maye interact? May need other method for that.
        //also flags or ints for how many other standard things like times of day etc.

        //should still be void, too complicated an output, adjust internal variables instead.
    }

    //maybe separate method to link variables interactive?
    //readout methods, tell me how many folks relative to the allocated number (so can map to IDs) are that test group
    //or maybe readout group numbers and get the variables that go with it, a bit more consistent and simple? Maybe both.
}
