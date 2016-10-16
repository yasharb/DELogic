package dataeaglealpha;

//@author Gavin Jenkins 2016
public class DataEagleAlpha {

    public static void main(String[] args) {
        DBUtility dbu = new DBUtility();
        //maybe have it read directories from wherever IT is, or from a config or whatever later.
        dbu.createNewDatabase("test.db", "jdbc:sqlite:C:/Users/Gavin/Dropbox/VancouverStuff/DECode/Databases/");
        dbu.csvToDatabase("C:/Users/Gavin/Dropbox/VancouverStuff/DECode/Databases/Fruitcraft.txt","jdbc:sqlite:C:/Users/Gavin/Dropbox/VancouverStuff/DECode/Databases/test.db" );
        
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
