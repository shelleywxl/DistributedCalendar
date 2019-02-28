/**
 * EventRecord object stores information of an event.
 */

public class EventRecord {
    
    private String opeartion;
    private int clock;
    private int nodeId;
    private Appointment appointment;
    
    public EventRecord(String operation, int clock, int nodeId) {
        this.opeartion = operation;
        this.clock = clock;
        this.nodeId = nodeId;
    }
    
    /**
     * Constructor when creating an event while restoring a crashed node's state.
     * @param operation "INSERT" or "DELETE"
     * @param clock time when the appointment is created, according to creator's clock
     * @param nodeId ID of node that creates the event
     * @param appointment appointment of this event
     */
    public EventRecord(String operation, int clock, int nodeId, Appointment appointment) {
        this.opeartion = operation;
        this.clock = clock;
        this.nodeId = nodeId;
        this.appointment = appointment;
    }
    
    public String getOperation() {
        return this.opeartion;
    }
    
    public int getClock() {
        return this.clock;
    }
    
    public int getNodeId() {
        return this.nodeId;
    }
    
    public Appointment getAppointment() {
        return this.appointment;
    }
    
}
