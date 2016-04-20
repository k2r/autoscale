package storm.psrl.scheduler;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 *
 * @author Roland
 */
public class ResourceMonitor {
    
	private static Logger logger = Logger.getLogger("ResourceMonitor");
    private Cluster cluster;
    private ArrayList<SupervisorDetails> overUsed;
    private ArrayList<SupervisorDetails> underUsed;
    private ArrayList<String> overUsedHosts;
    private ArrayList<String> underUsedHosts;
    
    public ResourceMonitor(Cluster cluster) throws IOException{
    	this.cluster = cluster;
    	this.overUsed = new ArrayList<>();
    	this.underUsed = new ArrayList<>();
    	this.overUsedHosts = new ArrayList<>();
    	this.underUsedHosts = new ArrayList<>();
    	
    	ArrayList<String> logs = new ArrayList<>();
    	Path path = Paths.get("resourceMonitor.log");
    	if(Files.exists(path)){
            logs = (ArrayList<String>) Files.readAllLines(path);
        }else{
            Files.createFile(path);
        }
    	
    	Map<String, SupervisorDetails> supIdToSupervisor = cluster.getSupervisors();
    	for(String supId : supIdToSupervisor.keySet()){
    		SupervisorDetails sd = supIdToSupervisor.get(supId);
    		Double cpuUsage = sd.getCpu();
    		Double memUsage = sd.getMemory();
    		if(cpuUsage != null && memUsage != null){
    			ResourceMonitor.logger.fine(supId + " cpu usage = " + cpuUsage + " memory usage = " + memUsage);
    			String line = supId + " cpu usage = " + cpuUsage + " memory usage = " + memUsage;
    			logs.add(line);
    			if(cpuUsage < 0.1){
    				this.underUsed.add(sd);
    			}else{
    				if(cpuUsage > 0.8 || memUsage > 0.8){
    					this.overUsed.add(sd);
    					if(this.underUsed.contains(sd)){
    						this.underUsed.remove(sd);
    					}
    				}
    			}
    		}	
    	}
    	for(SupervisorDetails ou : overUsed){
			this.overUsedHosts.add(ou.getHost());
		}
		for(SupervisorDetails uu : underUsed){
			this.underUsedHosts.add(uu.getHost());
		}
    	Files.write(path, logs, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
    }
    
    public ArrayList<SupervisorDetails> getOverUsed(){
        return this.overUsed;
    }
    
    public ArrayList<SupervisorDetails> getUnderUsed(){
        return this.underUsed;
    }
    
    public ArrayList<String> getOverUsedHosts(){
        return this.overUsedHosts;
    }
    
    public ArrayList<String> getUnderUsedHosts(){
        return this.underUsedHosts;
    }
    
    public void persistAllocations() throws IOException{
        Map<String, SchedulerAssignment> assignments = this.cluster.getAssignments();
        for(String topologyId : assignments.keySet()){
            Path path = Paths.get(topologyId);
            if(!Files.exists(path)){
                Files.createFile(path);
            }
            ArrayList<String> lines = new ArrayList<>();
            SchedulerAssignment assignment = assignments.get(topologyId);
            Set<WorkerSlot> workers = assignment.getSlots();
            String header = topologyId + " assigned to workers ";
            lines.add(header);
            for(WorkerSlot ws : workers){
                String line = ws.getNodeId() + ":" + ws.getPort() + ";";
                lines.add(line);
            }
            Files.write(path, lines, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        }
    }
}