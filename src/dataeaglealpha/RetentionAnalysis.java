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
    private Double[] highScores;
    private Integer[] bottleNecks;

    public RetentionAnalysis(String analysisName) {
        load(analysisName);
    }

    public RetentionAnalysis() {
        analyze();
    }

    public void save(String analysisName) {
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

    private void load(String analysisName) {
        //save the collection of mobileevents
        try {
            FileInputStream fis = new FileInputStream(analysisName + ".ser");
            ObjectInputStream ois = new ObjectInputStream(fis);
            eventsList = (HashMap) ois.readObject();
            ois.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        calcBottle(1);
    }

    public void calcBottle(Integer numBack) {
        calcBottle(numBack, (String)null);
    }

    public void calcBottle(Integer numBack, String... toIgnore) {
        //find the highest scoring one
        MobileEvent event;
        Iterator it = eventsList.entrySet().iterator();
        highScores = new Double[numBack];
        bottleNecks = new Integer[numBack];
        for (int i = 0; i < numBack; i++) {
            highScores[i] = (double)0;
            bottleNecks[i] = -1;
        }
        Boolean ignore;
        while (it.hasNext()) {
            HashMap.Entry entry = (HashMap.Entry) it.next();
            event = (MobileEvent) entry.getValue();
            ignore = false;
            if (toIgnore != null) {
                for (int i = 0; i < toIgnore.length; i++) {
                    if (DBUtility.getEventToString().containsKey(toIgnore[i])) {
                        ignore = true;
                    }
                }
            }
            //System.out.println(ignore);
            if (!ignore) {
                for (int i = 0; i<highScores.length; i++){
                    if ((event.getScore()) > highScores[i]){
                        highScores[i] = event.getScore();
                        bottleNecks[i] = event.getEventID();
                        break;
                    }
                }
            }
        }
    }

    public Integer[] getBottlenecks() {
        return bottleNecks;
    }

    public String[] getBottleneckNames() {
        String[] names = new String[bottleNecks.length];
        for (int i = 0; i<bottleNecks.length; i++){
            names[i] = DBUtility.getEventToString().get(bottleNecks[i]);
        }
        return names;
    }

    public Double[] getHighScores() {
        return highScores;
    }

    public final void analyze() {
        MobileEvent event;
        System.out.println(DBUtility.getMaxUserId());
        for (int i = 0; i <= DBUtility.getMaxUserId(); i++) { //iterate over users
            Integer[][] userArray = DBUtility.getEventsForUser(i);
            for (int j = 0; j < userArray.length; j++) { //iterate over events
                if (j < userArray.length) { //user went somewhere after this event
                    if (eventsList.containsKey(userArray[j][2])) {
                        System.out.println("1");
                        event = eventsList.get(j);
                        event.add(userArray[j][0], userArray[j + 1][2]);
                        event.scoreCalc();
                    } else {
                        System.out.println("2");
                        event = new MobileEvent(userArray[j][2]);
                        event.add(userArray[j][0], userArray[j + 1][2]);
                        event.scoreCalc();
                        eventsList.put(userArray[j][2], event);
                    }
                } else { //user did not go anywhere after this event, dead end.
                    if (eventsList.containsKey(userArray[j][2])) {
                        System.out.println("3");
                        event = eventsList.get(j);
                        event.add(userArray[j][0]);
                        event.scoreCalc();
                    } else {
                        System.out.println("4");
                        event = new MobileEvent(userArray[j][2]);
                        event.add(userArray[j][0]);
                        event.scoreCalc();
                        eventsList.put(userArray[j][2], event);
                    }
                }
            }
        }
        calcBottle(1);
    }
}
