package dataeaglealpha;

//@author Gavin Jenkins 2016
public class DataEagleAlpha {

    public static void main(String[] args) {

        if (args.length < 1) {
            //maybe have it read directories from wherever IT is, or from a config or whatever later.
            System.out.println("No input arguments provided, defaulting to loading existing DB");
            DBUtility.loadDatabase("jdbc:sqlite:./appEvents.db");

        } else {

            String csvFileAdd = "./sibche2.csv";
            String dbUrl = "jdbc:sqlite:./appEvents.db";

            if (args.length < 2) {
                System.out.println("CSV file address not provided, using default test file 'sibche2.csv'.");
            } else {
                csvFileAdd = args[1];
            }

            switch (args[0]) {
                case "add":
                    DBUtility.appendCSVToDatabase(csvFileAdd, dbUrl);
                    break;
                case "new":
                    DBUtility.setupDatabaseForNewCSV(csvFileAdd, dbUrl);
                    break;
            }
        }

        //testing
        RetentionAnalysis ret = new RetentionAnalysis();
        ret.calcBottle(3, "willResignActive", "didEnterBackground");
        System.out.println(ret.getHighScores());
        System.out.println(ret.getBottlenecks());

        //Start for now with a variable of interest, more like ARPU case
        //Find correlations in general for users?
        //Find immediately preceding events
        //Find dips and rises in the variable of interest over moving windows
        //Once picked some predictor events, try and start working out methods for power estimates (frequentist? Bayesian?)
        //divide out test groups
        //Make method for measuring for confidence of any effect as if we already did an intervention, Bayesian or frequentist
        //Make methods for tracking flag events
        //(All just playing around stuff for now)
    }
}
