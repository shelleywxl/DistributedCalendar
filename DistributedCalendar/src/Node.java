/**
 *
 * Node class.
 * Each Node corresponds to a single user.
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
    private String[][][] calendar;
    private int[][] T;  // 2-dimensional time table
    private Set<EventRecord> PL;  // Partial Log
    private Set<EventRecord> NE;  
    // At each receive event, a node extracts NE of which it has not yet learned from NP
    private Set<EventRecord> NP;
    // NP:={eR|eR belong to Li and there exists a node in the sending destinations k that not hasrec(Ti, eR, k)}
    private HashMap<String, Appointment> currentAppts;  // dictionary (Vi in the algorithm), key: apppointment ID
    private boolean[] sendFail;  // keep track of sending message
    
    private int apptNo;  
    // For appointment id. The number of appointments that are created by this 
    // node, increment the number after creating a new Appointment
    
    private String nodeStateFile;
    // Calendar element: null (default) means vacant; or appointment ID
    private static final String CALENDAR_VACANT = null;
    // Simplify as a calendar which spans 7 days and in 30 minute increments.
    private static final int CALENDAR_DAYS = 7;
    private static final int CALENDAR_TIMESLOTS = 48;
    // Event Record operations:
    private static final String ER_OP_INSERT = "insert";
    private static final String ER_OP_DELETE = "delete";
    // Send/Receive message operations:
    private static final int MSG_SEND_LOG = 0;
    private static final int MSG_DELETE_CONFLICT = 1;
    
    /**
     * Constructor of Node.
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
        this.calendar = new String[numNodes][CALENDAR_DAYS][CALENDAR_TIMESLOTS];
        this.T = new int[numNodes][numNodes];
        this.PL = new HashSet<>();
        this.NE = new HashSet<>();
        this.NP = new HashSet<>();
        this.currentAppts = new HashMap<>();
        
        this.apptNo = 0;  
        
        this.nodeStateFile = nodeId + "node_state.txt";
        
        // Track if this node sends message to other nodes successfully
        this.sendFail = new boolean[this.numNodes];
        for (int i = 0; i < sendFail.length; i++) {
            sendFail[i] = false;
        }
        
        // For failure recovery
        restoreNodeState();
    }
    
    /**
     * Create a new appointment (the participant can be himself only).
     * Check the local copy of the calendar. If no conflicts with all participants, 
     * add the meeting to site i's calendar and the event record to site i's log. 
     * Then send a message with site i's partial log to all other participants.
     * Those participants then update their logs and calendars.
     * @param apptName Name of the appointment
     * @param apptDay Date of the appointment. format: yyyyMMdd
     * @param apptStartTime HHmm in 24 hrs and 30 minutes increment, eg. "1930"
     * @param apptEndTime 
     * @param participants
     */
    public void createNewAppointment(String apptName, String apptDay, 
            String apptStartTime, String apptEndTime, ArrayList<Integer> participants) {
        
        boolean conflict = false;
        int startTimeIndex = Appointment.getApptTimeIndex(apptStartTime);
        int endTimeIndex = Appointment.getApptTimeIndex(apptEndTime);
        int dayIndex = Appointment.getApptDayIndex(apptDay);
        
        // Check local copy of calendar for the participants' availability.
        outerloop:
        for (int participant:participants) {
            synchronized(lock) {
                for (int t = startTimeIndex; t < endTimeIndex; t++) {
                    if (this.calendar[participant][dayIndex][t] != CALENDAR_VACANT) {
                        conflict = true;
                        break outerloop;
                    }
                }
            }
        }
        
        // According to the local copy of calendar, every participant is available.
        if (!conflict) {
            Appointment newAppointment = new Appointment(this.nodeId + "-" + this.apptNo, 
                    apptName, apptDay, apptStartTime, apptEndTime, participants, this.nodeId);
            this.apptNo++;
            
            // Add the appointment to local calendar
            for (int participant:participants) {
                synchronized(lock) {
                    for (int t = startTimeIndex; t < endTimeIndex; t++) {
                        this.calendar[participant][dayIndex][t] = newAppointment.getApptId();
                    }
                }
            }
            
            // Add the event record to log
            insert(newAppointment);
            
            // Send partial log to all other participants
            if (participants.size() > 1) {
                for (int participant:participants) {
                    if (participant != this.nodeId) {
                        send(participant, newAppointment, MSG_SEND_LOG);
                    }
                }
            }
        }
    }
    
    /**
     * The user can cancel an scheduled appointment it created.
     * Update the local calendar and add the event to the log.
     * Then send message to other participants.
     * @param apptId the unique id of the appointment to be deleted
     */
    public void deleteAppointment(String apptId) {
        Appointment deletedAppt = null;
        synchronized(lock) {
            // Get the scheduled appointment which to be deleted
            for (String deleteId:this.currentAppts.keySet()) {
                if (deleteId.equals(apptId)) {
                    deletedAppt = currentAppts.get(deleteId);
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
                        this.calendar[participant][dayIndex][i] = CALENDAR_VACANT;
                    }
                }
                
                // Send message to all other participants
                if (deletedAppt.getParticipants().size() > 1) {
                    for (int participant:deletedAppt.getParticipants()) {
                        if (participant != this.nodeId) {
                            send(participant, deletedAppt, MSG_SEND_LOG);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * From Wuu & Bernstein algorithm.
     * @param Ti Node-i's time table
     * @param eR event record
     * @param k Node k
     * @return true at Node-i if Node k has learned of the event
     */
    public boolean hasRec(int[][] Ti, EventRecord eR, int k) {
        return Ti[k][eR.getERNodeId()] >= eR.getERClock();
    }
    
    /**
     * From Wuu & Bernstein Algorithm.
     * Insert appointment into dictionary, and add the event record to log.
     * @param appointment the appointment to be added
     */
    public void insert(Appointment appointment) {
        this.clock++;
        this.T[this.nodeId][this.nodeId] = this.clock;
        EventRecord eR = new EventRecord(ER_OP_INSERT, this.clock, this.nodeId, appointment);
        addToLog(eR);
        synchronized(lock) {
            PL.add(eR);
            currentAppts.put(appointment.getApptId(), appointment);
            saveNodeState();
        }
    }
    
    /**
     * From Wuu & Bernstein Algorithm.
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
            for (String id:currentAppts.keySet()) {
                if (id.equals(appointment.getApptId())) {
                    currentAppts.remove(id);
                }
            }
            saveNodeState();
        }
    }
    
    /**
     * Communicative method.
     * Case A: Wuu & Bernstein: Creates NP, then sends <NP, T> to destination node.
     * Case B: Notify the initiator node that the appointment it creates 
     *          conflicts with the schedule.
     * @param destinationNode the node to which send the message
     * @param appt for case B, the appointment that is detected conflict
     * @param message to determine which case
     */
    private void send(final int destinationNode, Appointment appt, int message) {
        
        // For case A, update NP
        if (message == MSG_SEND_LOG) {
            this.NP.clear();
            synchronized(lock) {
                for (EventRecord eR:this.PL) {
                    if (!hasRec(this.T, eR, destinationNode)) {
                        this.NP.add(eR);
                    }
                }
                saveNodeState();
            }
        }
        
        try {
            Socket socket = new Socket(hostNames[destinationNode], port);
            OutputStream out = socket.getOutputStream();
            ObjectOutputStream objectOutput = new ObjectOutputStream(out);
            objectOutput.writeInt(message);
            
            synchronized(lock) {
                switch (message) {
                    case MSG_SEND_LOG:
                        objectOutput.writeObject(this.NP);
                        objectOutput.writeObject(this.T);
                        break;
                        
                    case MSG_DELETE_CONFLICT:
                        objectOutput.writeObject(appt);
                }
            }
            
            objectOutput.writeInt(this.nodeId);
            objectOutput.close();
            out.close();
            socket.close();
            sendFail[destinationNode] = false;
        }
        catch (ConnectException | UnknownHostException e) {
            // Create new thread to keep trying to send
            if (!sendFail[destinationNode]) { // start if not started yet
                sendFail[destinationNode] = true;
                Runnable runnable = new Runnable() {
                    public synchronized void run() {
                        while (sendFail[destinationNode]) {
                            try {
                                Thread.sleep(10000);
                                send(destinationNode, appt, message);
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

    /**
     * Communicative method.
     * Case A: Wuu & Bernstein: Receives <NP, T> from sender node. Update NE, 
     *      dictionary V, T, and PL.
     * Case B: Initiator of the appointment receives the conflict message. It 
     *      deletes the appointment as if explicitly cancels the appointment.
     * @param clientSocket 
     */
    public void receive(Socket clientSocket) {
        Set<EventRecord> NPk = null;
        int[][] Tk = null;
        Appointment deletedAppt = null;
        int senderNode = -1;
        int message = -1;

        
        try {
            InputStream in = clientSocket.getInputStream();
            ObjectInputStream objectInput = new ObjectInputStream(in);
            message = objectInput.readInt();
            
            switch (message) {
                case MSG_SEND_LOG:
                    NPk = (HashSet<EventRecord>)objectInput.readObject();
                    Tk = (int[][])objectInput.readObject();
                    break;
                    
                case MSG_DELETE_CONFLICT:
                    deletedAppt = (Appointment)objectInput.readObject();
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
        
        switch (message) {
            case MSG_SEND_LOG:
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
                        for  (EventRecord er:NE) {
                            addToLog(er);
                        }
                        
                        // 1) Check events in NE that to be deleted
                        HashSet<String> deleteApptIds = new HashSet<>();
                        for (String apptId:currentAppts.keySet()) {
                            for (EventRecord dR:NE) {
                                if (dR.getERAppointment().getApptId().equals(apptId) &&
                                        dR.getEROperation().equals(ER_OP_DELETE)) {
                                    deleteApptIds.add(apptId);
                                    // Update calendar
                                    int startIndex = Appointment.getApptTimeIndex(dR.getERAppointment().getStartTime());
                                    int endIndex = Appointment.getApptTimeIndex(dR.getERAppointment().getEndTime());
                                    int dayIndex = Appointment.getApptDayIndex(dR.getERAppointment().getDay());
                                    for (Integer participant:dR.getERAppointment().getParticipants()) {
                                        for (int i = startIndex; i < endIndex; i++) {
                                            this.calendar[participant][dayIndex][i] = CALENDAR_VACANT;
                                        }
                                    }
                                }
                            }
                        }
                        // Update dicionary
                        for (String apptId:deleteApptIds) {
                            currentAppts.remove(apptId);
                        }
                        
                        // 2) Check events in NE that to be inserted into dictionary
                        for (EventRecord er:NE) {
                            boolean deletionExists = false;
                            if (er.getEROperation().equals(ER_OP_INSERT)) {
                                Appointment newAppt = er.getERAppointment();
                                // If there is any delete operations on this appointment, do not add
                                for (EventRecord dR:NE) {
                                    if (dR.getERAppointment().getApptId().equals(newAppt.getApptId()) &&
                                        dR.getEROperation().equals(ER_OP_DELETE)) {
                                        deletionExists = true;
                                    }
                                }
                                if (!deletionExists) {
                                    int startIndex = Appointment.getApptTimeIndex(newAppt.getStartTime());
                                    int endIndex = Appointment.getApptTimeIndex(newAppt.getEndTime());
                                    int dayIndex = Appointment.getApptDayIndex(newAppt.getDay());
                                    // This node is a participant. Check time conflict first.
                                    if (newAppt.getParticipants().contains(this.nodeId)) {
                                        boolean conflict = false;
                                        for (int t = startIndex; t < endIndex; t++) {
                                            if (this.calendar[this.nodeId][dayIndex][t] != CALENDAR_VACANT) {
                                                conflict = true;
                                            }
                                        }
                                        if (conflict) {
                                            System.out.println("The new appointment conflicts with my schedule.");
                                            // Notify senderNode that this appointment needs to be cancelled
                                            send(senderNode, newAppt, MSG_DELETE_CONFLICT);
                                        }
                                        else {
                                            // Update local dictionary and calendar
                                            currentAppts.put(newAppt.getApptId(), newAppt);
                                            for (int participant:newAppt.getParticipants()) {
                                                for (int t = startIndex; t < endIndex; t++) {
                                                    this.calendar[participant][dayIndex][t] = newAppt.getApptId();
                                                }
                                            }
                                        }
                                    }
                                    // This node is not a participant. Update local dicionary and calendar. No need to check conflict.
                                    else {
                                        currentAppts.put(newAppt.getApptId(), newAppt);
                                        for (int participant:newAppt.getParticipants()) {
                                            for (int t = startIndex; t < endIndex; t++) {
                                                this.calendar[participant][dayIndex][t] = newAppt.getApptId();
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
                        HashSet<EventRecord> removePL = new HashSet();
                        for (EventRecord eR:PL){
                            boolean remove = true;
                            for (int s = 0; s < numNodes; s++){
                                if (!hasRec(T, eR, s)){
                                    remove = false;
                                }
                            }
                            if (remove)
                                removePL.add(eR);
                        }
                        for (EventRecord eR:removePL) {
                            PL.remove(eR);
			}
					
			for (EventRecord eR:NE){
                            for (int s = 0; s < numNodes; s++){
				if (!hasRec(T, eR, s)) {
                                    PL.add(eR);
				}
                            }
			}
                        
                        saveNodeState();
                    }
                }   
                break;
                
            // This node sent new appt to the senderNode. Then the senderNode 
            // found conflict in its schedule. This node should delete this appt, 
            // and notify all the participants.
            case MSG_DELETE_CONFLICT:
                if (deletedAppt != null) {
                    deleteAppointment(deletedAppt.getApptId());
                }
                break;
                
            default:
                break;
        }
    }
    
    /**
     * Add the event record to the log.
     * @param eR the EventRecord to be added to the log
     */
    private void addToLog(EventRecord eR) {
        if (!this.log.contains(eR)) {
            this.log.add(eR);
        }
    }
    
    /**
     * Save node state for recovering from failure.
     */
    private void saveNodeState() {
        try {
            FileOutputStream fos = new FileOutputStream(nodeStateFile);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            synchronized(lock) {
                oos.writeObject(this.clock);
                oos.writeObject(this.calendar);
                oos.writeObject(this.T);
                oos.writeObject(this.PL);
                oos.writeObject(this.NP);
                oos.writeObject(this.NE);
                oos.writeObject(this.currentAppts);
                oos.writeObject(this.apptNo);
            }
                oos.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Restore node state for recovering from failure.
     */
    private void restoreNodeState() {
        try {
            FileInputStream fis = new FileInputStream(nodeStateFile);
            ObjectInputStream ois = new ObjectInputStream(fis);
            this.clock = (int) ois.readObject();
            this.calendar = (String[][][]) ois.readObject();
            this.T = (int[][]) ois.readObject();
            this.PL = (Set<EventRecord>) ois.readObject();
            this.NP = (Set<EventRecord>) ois.readObject();
            this.NE = (Set<EventRecord>) ois.readObject();
            this.currentAppts = (HashMap<String, Appointment>) ois.readObject();
            this.apptNo = (int) ois.readObject();
        }
        catch (FileNotFoundException fnfe) {
            saveNodeState();
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    public String[][][] getCalendar() {
        return this.calendar;
    }

}
