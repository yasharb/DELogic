/*
 * All Rights Reserved
 */
package dataeaglealpha;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;

/**
 *
 * @author Gavin
 */
public class Retention {

    public static Integer Analyze() {
        HashMap<Integer, MobileEvent> eventsList = new HashMap();
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
        //find the highest scoring one
        Iterator it = eventsList.entrySet().iterator();
        double highScore = 0;
        int bottleNeck = -1;
        while (it.hasNext()) {
            HashMap.Entry entry = (HashMap.Entry) it.next();
            event = (MobileEvent) entry.getValue();
            if (event.getScore() > highScore) {
                highScore = event.getScore();
                bottleNeck = event.getEventID();
            }
        };

        //save the collection of mobileevents (for future purposes, could have methods that just load this as a starting point but not yet)
        try {
            FileOutputStream fos = new FileOutputStream("eventsList.ser");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(eventsList);
            oos.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return bottleNeck;
        }        
    }
}
