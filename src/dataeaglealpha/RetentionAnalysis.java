/*
 * All Rights Reserved
 */
package dataeaglealpha;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Iterator;

/**
 *
 * @author Gavin
 */
public class RetentionAnalysis {
    
    private HashMap<Integer, MobileEvent> eventsList = new HashMap();
    private double highScore = 0;
    private int bottleNeck = -1;
        
    public RetentionAnalysis(String analysisName){
        load(analysisName);
    }
    
    public RetentionAnalysis(){
        analyze();
    }
        
    public void save(String analysisName){
        //save the collection of mobileevents (for future purposes, could have methods that just load this as a starting point but not yet)
        try {
            FileOutputStream fos = new FileOutputStream(analysisName + ".ser");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(eventsList);
            oos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }    
    }
    
    private void load(String analysisName){
        //save the collection of mobileevents
        try {
            FileInputStream fis = new FileInputStream(analysisName + ".ser");
            ObjectInputStream ois = new ObjectInputStream(fis);
            eventsList = (HashMap) ois.readObject();
            ois.close();
        } catch (Exception e) {
            e.printStackTrace();
        }  
        calcBottle();
    }
    
    private void calcBottle(){
        //find the highest scoring one
        MobileEvent event;
        Iterator it = eventsList.entrySet().iterator();
        highScore = 0;
        bottleNeck = -1;
        while (it.hasNext()) {
            HashMap.Entry entry = (HashMap.Entry) it.next();
            event = (MobileEvent) entry.getValue();
            if (event.getScore() > highScore) {
                highScore = event.getScore();
                bottleNeck = event.getEventID();
            }
        }
    }
    
    public Integer getBottleneck(){
        return bottleNeck;
    }
    
    public Double getHighScore(){
        return highScore;
    }

    public final void analyze() {
        MobileEvent event;
        for (int i = 0; i < DBUtility.getMaxUserId(); i++) { //iterate over users
            Integer[][] userArray = DBUtility.getEventsForUser(i);
            for (int j = 0; j < userArray.length; j++) { //iterate over events
                if (j < userArray.length) { //user went somewhere after this event
                    if (eventsList.containsKey(userArray[j][2])) {
                        event = eventsList.get(j);
                        event.add(userArray[j][0], userArray[j + 1][2]);
                        event.scoreCalc();
                    } else {
                        event = new MobileEvent(userArray[j][2]);
                        event.add(userArray[j][0], userArray[j + 1][2]);
                        event.scoreCalc();
                        eventsList.put(userArray[j][2], event);
                    }
                } else { //user did not go anywhere after this event, dead end.
                    if (eventsList.containsKey(userArray[j][2])) {
                        event = eventsList.get(j);
                        event.add(userArray[j][0]);
                        event.scoreCalc();
                    } else {
                        event = new MobileEvent(userArray[j][2]);
                        event.add(userArray[j][0]);
                        event.scoreCalc();
                        eventsList.put(userArray[j][2], event);
                    }
                }
            }
        }
        calcBottle();
    }
}
