/**
 *
 * Node object
 */
public class Node {
    
    private int nodeId;
    private int numNodes;
    private int port;
    private String[] hostNames;
    
    private int clock;
    private int[][][] calendar;
    private int[][] T;
    
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
        
        this.clock = 0;
        this.calendar = new int[numNodes][7][48];
        this.T = new int[numNodes][numNodes];
        
    }
    
    /*
     * Returns whether Node k has learned about event e
     */
    public boolean hasRec(int[][] Ti, EventRecord eR, int k) {
        return this.T[k][eR.getNodeId()] >= eR.getTime();
    }
    
    /*
     * Insert appointment into dictionary.
     */
    public void insert(Appointment appointment) {
        // TODO: handle is_calendar_conflicting?
        this.clock += 1;
        this.T[this.nodeId][this.nodeId] = this.clock;
        EventRecord eR = new EventRecord("INSERT", this.clock, this.nodeId, appointment);
        writeToLog(eR);
    }
    
    /*
     * Delete the appointment from dictionary.
    */
    public void delete() {
        
    }
    
    public void send() {
        
    }
    
    public void receive() {
        
    }
    
    private void writeToLog(EventRecord eR) {
        // if eR not in log yet, then add?
        try {
            
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
