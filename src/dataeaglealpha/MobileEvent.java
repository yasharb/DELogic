/*
 * All Rights Reserved
 */
package dataeaglealpha;

import java.util.HashMap;

/**
 *
 * @author Gavin
 */
public class MobileEvent {
    private HashMap<Integer,Integer> links  = new HashMap(); //key = event ID, payload = count of people taking that path so far.
    private HashMap<Integer, Integer> users = new HashMap(); // key = unique user IDs, payload = number of redundant visits by that user.
    private Integer numVisitors = 0; // total number of visitors to this event (number of times Add has been called)
    private Integer numLinks = 0; // number of times somebody went somewhere from this event.
    private Integer eventID;
    private Double importance = (double)0;
    private Double lossRate = (double)1;
    private Double score = (double)0;
    private Double expConstant = (double) 10; //higher = more gradual falloff of exponent function for redundant visits. Optional constructor to specify.
    private Double uniqueScore = (double) 5; //raw score for first unique visit to the event for a user. Optional constructor to specify.
    //initial visit currently hardcoded as 5 points by comparison.
    
    public void MobileEvent (Integer eventIDInput){
        eventID = eventIDInput;
    }
    
    public void MobileEvent (Integer eventIDInput, Double exp_Constant){
        eventID = eventIDInput;
        expConstant = exp_Constant;
    }
    
    public void MobileEvent (Integer eventIDInput, Double exp_Constant, Double unique_Score){
        eventID = eventIDInput;
        expConstant = exp_Constant;
        uniqueScore = unique_Score;
    }
    
    public void Add (Integer userID, Integer nextEventID){
        //update hashes, numVisitors, importance.
        if (links.containsKey(nextEventID)){
            int numVisits = links.get(nextEventID);
            links.replace(nextEventID, numVisits++);
        } else{
            links.put(nextEventID,1);
        }
        if (users.containsKey(userID)){
            int numVisits = users.get(userID);
            users.replace(userID, numVisits++);
            importance = importance + Falloff(numVisits);
        } else{
            links.put(userID,1);
            importance = importance + Falloff(1);
        }
        numVisitors++;
        numLinks++;
    }
    
    public void Add (Integer userID){
        //update hashes, numVisitors, importance, for last event available for a user
        if (users.containsKey(userID)){
            int numVisits = users.get(userID);
            users.replace(userID, numVisits++);
            importance = importance + Falloff(numVisits);
        } else{
            links.put(userID,1);
            importance = importance + Falloff(1);
        }
        numVisitors++;
    }

    public void ScoreCalc(){
        lossRate = (double)(1 - (numLinks / numVisitors));
        score = lossRate * importance;
    }
    
    private Double Falloff(Integer numVisitsOneUser){
        if(numVisitsOneUser == 1){
            return (double) uniqueScore;
        }
        return 1/Math.exp(numVisitsOneUser/expConstant);
    }
    
    //getters
    public Integer getEventID(){
        return eventID;
    }
    public Double getImportance(){
        return importance;
    }
    public Double getLossRate(){
        return lossRate;
    }
    public Double getScore(){
        return score;
    }
    public Integer getNumVisitors(){
        return numVisitors;
    }
    public Integer getNumLinks(){
        return numLinks;
    }
}
