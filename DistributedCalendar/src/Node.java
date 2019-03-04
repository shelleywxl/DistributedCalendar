/**
 *
 * Node object
 */

import java.util.*;
import java.net.*;
import java.io.*;

public class Node {
    
    private int nodeId;
    private int numNodes;
    private int port;
    private String[] hostNames;
    private Set<EventRecord> log;
    
    private Object lock = new Object();
    private int clock;
    private int[][] T;  // 2-dimensional time table
    private int[][][] calendar;
    private Set<EventRecord> PL;  // Partial Log
    private Set<EventRecord> NE;  
    // At each receive event, a node extracts NE of which it has not yet learned from NP
    private Set<EventRecord> NP;
    // NP:={eR|eR belong to Li and there exists a node in the sending destinations k that not hasrec(Ti, eR, k)}
    private Set<Appointment> currentAppts;  // dictionary (Vi in the algorithm)
    private boolean[] sendFail;  // keep track of sending message
    
    private static final String NODE_STATE_FILE = "node_state.txt";
    // Event Record operations:
    private static final String ER_OP_INSERT = "insert";
    private static final String ER_OP_DELETE = "delete";
    // Send/Receive message operations:
    private static final int MSG_OP_INSERT = 0;
    private static final int MSG_OP_DELETE = 1;
    private static final int MSG_OP_DELETE_CONFLICT = 2;
    
    /**
     * 
     * @param nodeId
     * @param numNodes
     * @param port
     * @param hostNames 
     */
    public Node(int nodeId, int numNodes, int port, String[] hostNames) {
        this.nodeId = nodeId;
        this.numNodes = numNodes;
        this.port = port;
        this.hostNames = hostNames;
        this.log = new HashSet<>();
        
        this.clock = 0;
        this.calendar = new int[numNodes][7][48];
        this.T = new int[numNodes][numNodes];
        this.PL = new HashSet<>();
        this.NE = new HashSet<>();
        this.NP = new HashSet<>();
        this.currentAppts = new HashSet<>();
        
        // track if this node sends message to other nodes successfully
        this.sendFail = new boolean[this.numNodes];
        for (int i = 0; i < sendFail.length; i++) {
            sendFail[i] = false;
        }
        
        restoreNodeState();
    }
    
    /**
     * Create a new appointment (the participant can be himself only).
     * Check the local copy of the calendar. If no conflicts with all participants, 
     * add the meeting to site i's calendar and the event record to site i's log. 
     * Then send a message with site i's partial log to all other participants.
     * Those participants then update their logs and calendars.
     * @param apptName
     * @param apptDay
     * @param apptStartTime
     * @param apptEndTime 
     */
    public void createNewAppointment(ArrayList<Integer> participants, String apptName, String apptDay, int apptStartTime, int apptEndTime) {
        boolean conflict = false;
        int startTimeIndex = Appointment.getApptTimeIndex(apptStartTime);
        int endTimeIndex = Appointment.getApptTimeIndex(apptEndTime);
        int dayIndex = Appointment.getApptDayIndex(apptDay);
        // Check local copy of calendar:
        for (int participant:participants) {
            synchronized(lock) {
                for (int t = startTimeIndex; t < endTimeIndex; t++) {
                    if (this.calendar[participant][dayIndex][t] != 0) {
                        conflict = true;
                    }
                }
            }
            if (conflict) {
                break;
            }
        }

        if (!conflict) {
            // Add the appointment to local calendar
            for (int participant:participants) {
                synchronized(lock) {
                    for (int t = startTimeIndex; t < endTimeIndex; t++) {
                        this.calendar[participant][dayIndex][t] = 1;
                    }
                }
            }
            // Add the event record to log
            Appointment newAppointment = new Appointment(apptName, apptDay, apptStartTime, apptEndTime, participants, this.nodeId);
            insert(newAppointment);
            
            // Send partial log to all other participants
            if (participants.size() > 1) {
                for (int participant:participants) {
                    if (participant != this.nodeId) {
                        send(participant);
                    }
                }
            }
        }
    }
    
    /**
     * The user can cancel the appointment in which he/she is a participant.
     * Update the local calendar and add the event to the log.
     * Then send message to other participants.
     * @param apptId the unique id of the appointment to be deleted
     */
    public void deleteAppointment(int apptId) {
        Appointment deletedAppt = null;
        synchronized(lock) {
            for (Appointment appt:this.currentAppts) {
                if (appt.getApptId() == apptId) {
                    deletedAppt = appt;
                }
            }
            if (deletedAppt != null) {
                // Delete from dictionary and log the event
                delete(deletedAppt);
                // Update local calendar
                int dayIndex = Appointment.getApptDayIndex(deletedAppt.getDay());
                int startTimeIndex = Appointment.getApptTimeIndex(deletedAppt.getStartTime());
                int endTimeIndex = Appointment.getApptTimeIndex(deletedAppt.getEndTime());
                for (int participant:deletedAppt.getParticipants()) {
                    for (int i = startTimeIndex; i < endTimeIndex; i++) {
                        this.calendar[participant][dayIndex][i] = 0;
                    }
                }
                // Send message to all other participants
                if (deletedAppt.getParticipants().size() > 1) {
                    for (int participant:deletedAppt.getParticipants()) {
                        if (participant != this.nodeId) {
                            send(participant);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * If a node receives a "create" event where the appointment conflicts with 
     * an existing appointment, the node must delete the appointment, and send
     * message to other participants.
     * In the algorithm, the node may not always have the most up-to-date info 
     * about the other nodes' logs and dictionaries, so it may result in the 
     * scheduling of conflicting appointments.
     * @param delAppt 
     */
    public void deleteConflictAppointment(Appointment delAppt) {
        
    }
    
    /**
     * Returns true at Ni if Node k has learned of the event.
     */
    public boolean hasRec(int[][] Ti, EventRecord eR, int k) {
        return this.T[k][eR.getERNodeId()] >= eR.getERClock();
    }
    
    /**
     * Insert appointment into dictionary. Add the event record to log.
     * @param appointment the appointment to be added
     */
    public void insert(Appointment appointment) {
        // TODO: handle is_calendar_conflicting?
        this.clock++;
        this.T[this.nodeId][this.nodeId] = this.clock;
        EventRecord eR = new EventRecord(ER_OP_INSERT, this.clock, this.nodeId, appointment);
        addToLog(eR);
        synchronized(lock) {
            PL.add(eR);
            currentAppts.add(appointment);
            saveNodeState();
        }
    }
    
    /**
     * Delete the appointment from dictionary.
     * @param appointment the appointment to be deleted
     */
    public void delete(Appointment appointment) {
        this.clock++;
        this.T[this.nodeId][this.nodeId] = this.clock;
        EventRecord eR = new EventRecord(ER_OP_DELETE, this.clock, this.nodeId, appointment);
        addToLog(eR);
        synchronized(lock) {
            PL.add(eR);
            Appointment delAppt = null;
            for (Appointment appt:currentAppts) {
                if (appt.getApptId() == appointment.getApptId()) {
                    delAppt = appt;
                }
            }
            if (delAppt != null) {
                currentAppts.remove(delAppt);
            }
            saveNodeState();
        }
    }
    
    /**
     * Creates NP, then sends <NP, T> to node k.
     * @param k the node to which send the message. 
     */
    public void send(final int destinationNode) {
        NP.clear();
        synchronized(lock) {
            for (EventRecord eR:PL) {
                if (!hasRec(this.T, eR, destinationNode)) {
                    NP.add(eR);
                }
            }
            saveNodeState();
        }
        try {
            Socket socket = new Socket(hostNames[destinationNode], port);
            OutputStream out = socket.getOutputStream();
            ObjectOutputStream objectOutput = new ObjectOutputStream(out);
            objectOutput.writeInt(MSG_OP_INSERT);  // TODO: 0 means sending set of events, should convert to string final
            synchronized(lock) {
                objectOutput.writeObject(NP);
                objectOutput.writeObject(T);
            }
            objectOutput.writeInt(nodeId);
            objectOutput.close();
            out.close();
            sendFail[destinationNode] = false;
        }
        catch (ConnectException | UnknownHostException e) {
            // Create new thread to keep trying to send
            if (!sendFail[destinationNode]) {
                sendFail[destinationNode] = true;
                Runnable runnable = new Runnable() {
                    public synchronized void run() {
                        while (sendFail[destinationNode]) {
                            try {
                                Thread.sleep(10000);
                                send(destinationNode);
                            }
                            catch (InterruptedException ie) {
                                ie.printStackTrace();
                            }
                        }
                    }
                };
                new Thread(runnable).start();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /*
     * Receives <NP, T> from node senderNode.
     */
    public void receive(Socket clientSocket) {
        Set<EventRecord> NPk = null;
        int[][] Tk = null;
        int senderNode = -1;
        int operation = -1;
        Appointment deletedAppt = null;
        EventRecord deletedEr = null;
        
        try {
            InputStream in = clientSocket.getInputStream();
            ObjectInputStream objectInput = new ObjectInputStream(in);
            operation = objectInput.readInt();
            
            switch (operation) {
                case MSG_OP_INSERT:
                    NPk = (HashSet<EventRecord>)objectInput.readObject();
                    Tk = (int[][])objectInput.readObject();
                    break;
                case MSG_OP_DELETE:
                    deletedAppt = (Appointment)objectInput.readObject();
                    break;
                case MSG_OP_DELETE_CONFLICT:
                    deletedEr = (EventRecord)objectInput.readObject();
                    break;
                default:
                    break;
            }
            senderNode = objectInput.readInt();
            objectInput.close();
            in.close();
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        
        // Case 1: new appointment creation received
        switch (operation) {
            case MSG_OP_INSERT:
                if (NPk != null) {
                    synchronized(lock) {
                        // Update NE
                        NE.clear();
                        for (EventRecord fR:NPk) {
                            if (!hasRec(this.T, fR, this.nodeId)) {
                                NE.add(fR);
                            }
                        }
                        // Update the dictionary, calendar and log
                        HashSet<Appointment> deleteAppts = new HashSet<>();
                        for (Appointment appt:currentAppts) {
                            // get the EventRecord that contains appointment that to be deleted
                            for (EventRecord dR:NE) {
                                if (dR.getERAppointment().getApptId() == appt.getApptId() &&
                                        dR.getEROperation().equals(ER_OP_DELETE)) {
                                    deleteAppts.add(appt);
                                    // Update calendar
                                    int startIndex = Appointment.getApptTimeIndex(dR.getERAppointment().getStartTime());
                                    int endIndex = Appointment.getApptTimeIndex(dR.getERAppointment().getEndTime());
                                    int dayIndex = Appointment.getApptDayIndex(dR.getERAppointment().getDay());
                                    for (Integer participant:dR.getERAppointment().getParticipants()) {
                                        for (int i = startIndex; i < endIndex; i++) {
                                            this.calendar[participant][dayIndex][i] = 0;
                                        }
                                    }
                                }
                            }
                        }
                        // Update dicionary
                        for (Appointment appt:deleteAppts) {
                            currentAppts.remove(appt);
                        }
                        // Check events in NE that to be inserted into dictionary
                        for (EventRecord er:NE) {
                            boolean deletionExists = false;
                            addToLog(er);
                            if (er.getEROperation().equals(ER_OP_INSERT)) {
                                Appointment newAppt = er.getERAppointment();
                                // Check conflict, if there is any delete operations on this appointment, do nothing
                                for (EventRecord dR:NE) {
                                    if (dR.getERAppointment().getApptId() == newAppt.getApptId() &&
                                        dR.getEROperation().equals(ER_OP_DELETE)) {
                                        deletionExists = true;
                                    }
                                }
                                // Add this appointment.
                                if (!deletionExists) {
                                    int startIndex = Appointment.getApptTimeIndex(newAppt.getStartTime());
                                    int endIndex = Appointment.getApptTimeIndex(newAppt.getEndTime());
                                    int dayIndex = Appointment.getApptDayIndex(newAppt.getDay());
                                    // This node is a participant. Check time conflict first.
                                    if (newAppt.getParticipants().contains(this.nodeId)) {
                                        boolean conflict = false;
                                        for (int t = startIndex; t < endIndex; t++) {
                                            if (this.calendar[this.nodeId][dayIndex][t] == 1) {
                                                conflict = true;
                                            }
                                        }
                                        if (conflict) {
                                            // TODO: notify senderNode that this appointment needs to be cancelled
                                            // sendConflict(receivedAppt, senderNode);
                                        }
                                        else {
                                            currentAppts.add(newAppt);
                                            for (int participant:newAppt.getParticipants()) {
                                                for (int t = startIndex; t < endIndex; t++) {
                                                    this.calendar[participant][dayIndex][t] = 1;
                                                }
                                            }
                                        }
                                    }
                                    // This node is not a participant. Update local dicionary and calendar. No need to check conflict.
                                    else {
                                        currentAppts.add(newAppt);
                                        for (int participant:newAppt.getParticipants()) {
                                            for (int t = startIndex; t < endIndex; t++) {
                                                this.calendar[participant][dayIndex][t] = 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Update T
                        for (int r = 0; r < numNodes; r++) {
                            this.T[this.nodeId][r] = Math.max(this.T[this.nodeId][r], Tk[senderNode][r]);
                        }
                        for (int r = 0; r < numNodes; r++) {
                            for (int s = 0; s < numNodes; s++) {
                                this.T[r][s] = Math.max(this.T[r][s], Tk[r][s]);
                            }
                        }
                        
                        // Update PL
                        
                        for (EventRecord er:NE) {
                            
                        }
                        
                        saveNodeState();
                    }
                }   
                break;
            case MSG_OP_DELETE:
                break;
            case MSG_OP_DELETE_CONFLICT:
                break;
            default:
                break;
        }
    }
    
    /**
     * Add the event record to the log
     * @param eR the EventRecord to be added to the log
     */
    private void addToLog(EventRecord eR) {
        // if eR not in log yet, then add?
        if (!this.log.contains(eR)) {
            this.log.add(eR);
        }
    }
    
    private void saveNodeState() {
        
    }
    
    private void restoreNodeState() {
        
    }
    
    public int[][][] getCalendar() {
        return this.calendar;
    }
}
