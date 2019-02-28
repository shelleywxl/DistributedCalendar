/**
 *
 * Appointment object
 */

import java.util.ArrayList;

public class Appointment {
    private String name;
    private AppointmentDay day;
    private Integer startTime;
    private Integer endTime;
    private ArrayList<Integer> participants;
    private int initNode;
    
    /**
     * Constructor for creating a new appointment.
     * @param name name of the appointment
     * @param day day in the week of the appointment
     * @param startTime start time of the appointment, format example: "1930" is 7:30PM
     * @param endTime end time of the appointment
     * @param participants list of participants in the appointment
     * @param initNode ID of the Node who initialize the appointment
     */
    public Appointment(String name, AppointmentDay day, int startTime, int endTime, ArrayList<Integer> participants, int initNode) {
        this.name = name;
        this.day = day;
        this.startTime = startTime;
        this.endTime = endTime;
        this.participants = participants;
        this.initNode = initNode;
        
    }
    
    public boolean appointmentsConflict(Appointment appointment1, Appointment appointment2) {
        if (appointment1 == appointment2) {
            return false;
        }
        if (appointment1.day != appointment2.day) {
            return false;
        }
        if (appointment1.endTime <= appointment2.startTime || 
                appointment1.startTime >= appointment2.endTime) {
            return false;
        }
        for (int nodeId : appointment1.participants) {
            if (appointment2.participants.contains(nodeId)) {
                return true;
            }
        }
        return false;
    }
    
}
