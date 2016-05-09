/**
 * 
 */
package storm.autoscale.scheduler.modules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * @author Roland
 *
 */
public class AssignmentMonitor {

	private Cluster cluster;
	private TopologyDetails topology;
	private HashMap<WorkerSlot, ArrayList<String>> assignments;
	private HashMap<SupervisorDetails, ArrayList<WorkerSlot>> support;
	
	/**
	 * 
	 */
	public AssignmentMonitor(Cluster cluster, TopologyDetails topology) {
		this.cluster = cluster;
		this.topology = topology;
		this.assignments = new HashMap<>();
		this.support = new HashMap<>();
		List<WorkerSlot> slots = cluster.getAssignableSlots();
		for(WorkerSlot ws : slots){
			this.assignments.put(ws, new ArrayList<>());
			
			SupervisorDetails supervisor = this.cluster.getSupervisorById(ws.getNodeId());
			if(!this.support.containsKey(supervisor)){
				ArrayList<WorkerSlot> managedSlots = new ArrayList<>();
				managedSlots.add(ws);
				this.support.put(supervisor, managedSlots);
			}else{
				ArrayList<WorkerSlot> managedSlots = this.support.get(supervisor);
				managedSlots.add(ws);
				this.support.replace(supervisor, managedSlots);
			}
		}
		
	}
	
	public void update(){
		SchedulerAssignment schedAssignment = this.cluster.getAssignmentById(this.topology.getId());
		Map<ExecutorDetails, WorkerSlot> executorToSlots = schedAssignment.getExecutorToSlot();
		for(ExecutorDetails executor : executorToSlots.keySet()){
			WorkerSlot slot = executorToSlots.get(executor);
			String component = this.topology.getExecutorToComponent().get(executor);
			ArrayList<String> affectedComponents = this.assignments.get(slot);
			if(!affectedComponents.contains(component)){
				affectedComponents.add(component);
				this.assignments.replace(slot, affectedComponents);
			}
		}
	}
	
	public ArrayList<WorkerSlot> getFreeSlots(){
		return (ArrayList<WorkerSlot>) this.cluster.getAvailableSlots();
	}
	
	public ArrayList<SupervisorDetails> getFreeSupervisors(){
		ArrayList<SupervisorDetails> result = new ArrayList<>();
		Set<String> supervisors = cluster.getSupervisors().keySet();
		for(String supervisor : supervisors){// initialize the result with all the supervisors
			result.add(cluster.getSupervisors().get(supervisor));
		}
		Collection<WorkerSlot> usedSlots = this.cluster.getUsedSlots();
		for(WorkerSlot slot : usedSlots){
			SupervisorDetails supervisor = this.cluster.getSupervisorById(slot.getNodeId());
			if(result.contains(supervisor)){
				result.remove(supervisor);//substract supervisors with at least one busy slot
			}
		}
		return result;
	}

	public ArrayList<String> getFreeHosts(){
		ArrayList<String> result = new ArrayList<>();
		ArrayList<SupervisorDetails> freeSupervisors = this.getFreeSupervisors();
		for(SupervisorDetails freeSupervisor : freeSupervisors){
			result.add(freeSupervisor.getHost());
		}
		return result;
	}
	
	public ArrayList<WorkerSlot> getSharableSlots(String component){
		ArrayList<WorkerSlot> result = new ArrayList<>();
		Collection<WorkerSlot> usedSlots = this.cluster.getUsedSlots();
		for(WorkerSlot slot : usedSlots){
			ArrayList<String> assignedComponents = this.assignments.get(slot);
			if(!assignedComponents.contains(component)){
				result.add(slot);
			}
		}
		return result;
	}
	
	public Integer getProximity(String host, String component){
		Integer result = 0;
		TopologyExplorer explorer = new TopologyExplorer(this.topology.getTopology()); 
		List<SupervisorDetails> supervisors =  this.cluster.getSupervisorsByHost(host);
		for(SupervisorDetails supervisor : supervisors){
			ArrayList<WorkerSlot> slots = this.support.get(supervisor);
			for(WorkerSlot slot : slots){
				ArrayList<String> assignedComponents = this.assignments.get(slot);
				for(String assignedComponent : assignedComponents){
					if(explorer.areLinked(component, assignedComponent)){
						result++;
					}
				}
			}
		}
		return result;
	}
}
