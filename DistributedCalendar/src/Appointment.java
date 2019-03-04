/**
 *
 * Appointment object
 */

import java.util.ArrayList;

public class Appointment {
    private String name;
    private String day;
    private Integer startTime;
    private Integer endTime;
    private ArrayList<Integer> participants;
    private int initNode;
    private int apptId;
    private static Integer apptNo = 0;  // unique ID for the appointment
    
    /**
     * Constructor for creating a new appointment.
     * @param name name of the appointment
     * @param day date of the appointment, format example: YYYYMMDD "20190315"
     * @param startTime start time of the appointment, incremented by 30 minutes, format example: 1930 is 7:30PM
     * @param endTime end time of the appointment
     * @param participants list of participants in the appointment
     * @param initNode ID of the Node who initialize the appointment
     */
    public Appointment(String name, String day, int startTime, int endTime, ArrayList<Integer> participants, int initNode) {
        this.name = name;
        this.day = day;
        this.startTime = startTime;
        this.endTime = endTime;
        this.participants = participants;
//        this.initNode = initNode;
        setApptId(apptNo);
        Appointment.apptNo++;
    }
    
    /**
     * Convert the start/end time to an index to be put in the calendar.
     * @param time start/end time of the appointment, 0000->0 to 2330, incremented by 30
     * @return an integer from 0 to 23
     */
    public static Integer getApptTimeIndex(int time) {
        int index = time / 100 * 2;
        if (time % 100 == 30) {
            index += 1;
        }
        return index;
    }
    
    // Assume the appointment cannot span multiple days.
//    public boolean appointmentsConflict(Appointment appointment1, Appointment appointment2) {
//        if (appointment1 == appointment2) {
//            return false;
//        }
//        if (appointment1.day.equals(appointment2.day)) {
//            return false;
//        }
//        if (appointment1.endTime <= appointment2.startTime || 
//                appointment1.startTime >= appointment2.endTime) {
//            return false;
//        }
//        for (int nodeId : appointment1.participants) {
//            if (appointment2.participants.contains(nodeId)) {
//                return true;
//            }
//        }
//        return false;
//    }
    
    public String getName() {
        return this.name;
    }
    
    public String getDay() {
        return this.day;
    }
    
    public Integer getStartTime() {
        return this.startTime;
    }
    
    public Integer getEndTime() {
        return this.endTime;
    }
    
    public ArrayList<Integer> getParticipants() {
        return this.participants;
    }
    
    public final void setApptId(Integer apptId) {
        this.apptId =  apptId;
    }
    
    public Integer getApptId() {
        return this.apptId;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.name + "," + 
                this.day + "," + 
                this.startTime + "," + 
                this.endTime + ",");
        for (int i = 0; i < this.participants.size(); i++) {
            sb.append(Integer.toString(this.participants.get(i)));
            if (i != this.participants.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("\n");
        return sb.toString();
    }
}
