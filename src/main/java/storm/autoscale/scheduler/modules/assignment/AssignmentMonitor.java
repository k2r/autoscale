/**
 * 
 */
package storm.autoscale.scheduler.modules.assignment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;

import storm.autoscale.scheduler.modules.explorer.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class AssignmentMonitor {

	private Cluster cluster;
	private TopologyDetails topology;
	private HashMap<WorkerSlot, ArrayList<String>> assignments;
	private HashMap<String, ArrayList<WorkerSlot>> assignmentsPerComponent;
	private HashMap<SupervisorDetails, ArrayList<WorkerSlot>> support;
	
	private Logger logger = Logger.getLogger("AssignmentMonitor");
	
	/**
	 * 
	 */
	public AssignmentMonitor(Cluster cluster, TopologyDetails topology) {
		this.cluster = cluster;
		this.topology = topology;
		this.assignments = new HashMap<>();
		this.assignmentsPerComponent = new HashMap<>();
		this.support = new HashMap<>();
		List<WorkerSlot> slots = cluster.getAssignableSlots();
		for(WorkerSlot ws : slots){
			this.assignments.put(ws, new ArrayList<String>());
			SupervisorDetails supervisor = this.cluster.getSupervisorById(ws.getNodeId());
			if(!this.support.containsKey(supervisor)){
				ArrayList<WorkerSlot> managedSlots = new ArrayList<>();
				managedSlots.add(ws);
				this.support.put(supervisor, managedSlots);
			}else{
				ArrayList<WorkerSlot> managedSlots = this.support.get(supervisor);
				managedSlots.add(ws);
				this.support.remove(supervisor);
				this.support.put(supervisor, managedSlots);
			}
		}
		
	}
	
	public void update(){
		try{
			SchedulerAssignment schedAssignment = this.cluster.getAssignmentById(this.topology.getId());
			if(schedAssignment != null){
				Map<ExecutorDetails, WorkerSlot> executorToSlots = schedAssignment.getExecutorToSlot();
				for(ExecutorDetails executor : executorToSlots.keySet()){
					WorkerSlot slot = executorToSlots.get(executor);
					String component = this.topology.getExecutorToComponent().get(executor);
					ArrayList<String> affectedComponents = this.assignments.get(slot);
					if(!affectedComponents.contains(component)){
						affectedComponents.add(component);
						this.assignments.remove(slot);
						this.assignments.put(slot, affectedComponents);
					}
				}
				Set<WorkerSlot> workers = this.assignments.keySet(); 
				for(WorkerSlot ws : workers){
					ArrayList<String> components = this.getRunningComponents(ws);
					for(String component : components){
						ArrayList<WorkerSlot> slots = new ArrayList<>();
						if(this.assignmentsPerComponent.containsKey(component)){
							slots = this.assignmentsPerComponent.get(component);
						}
						if(!slots.contains(ws)){
							slots.add(ws);
						}
						this.assignmentsPerComponent.put(component, slots);
					}
				}
			}
		}catch(NullPointerException e){
			logger.info("No assignment defined yet for topology " + this.topology.getId());
		}
	}
	
	public Cluster getCluster(){
		return this.cluster;
	}
	
	public ArrayList<String> getRunningComponents(WorkerSlot worker){
		return this.assignments.get(worker);
	}
	
	public ArrayList<WorkerSlot> getAllocatedWorkers(String component){
		return this.assignmentsPerComponent.get(component);
	}
	
	public HashMap<SupervisorDetails, ArrayList<WorkerSlot>> getWorkers(){
		HashMap<SupervisorDetails, ArrayList<WorkerSlot>> result = new HashMap<>();
		Collection<SupervisorDetails> supervisors = this.cluster.getSupervisors().values();
		for(SupervisorDetails supervisor : supervisors){
			ArrayList<WorkerSlot> slots = (ArrayList<WorkerSlot>) this.cluster.getAssignableSlots(supervisor);
			result.put(supervisor, slots);
		}
		return result;
	}
	
	public SupervisorDetails getSupervisor(WorkerSlot ws, HashMap<SupervisorDetails, ArrayList<WorkerSlot>> supToWorkers){
		SupervisorDetails result = null;
		for(SupervisorDetails supervisor : supToWorkers.keySet()){
			ArrayList<WorkerSlot> workers = supToWorkers.get(supervisor);
			if(workers.contains(ws)){
				result = supervisor;
				break;
			}
		}
		return result;
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
	
	public Integer getNbWorkers(){
		return this.cluster.getAssignableSlots().size();
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
		TopologyExplorer explorer = new TopologyExplorer(this.topology.getName(), this.topology.getTopology()); 
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
	
	public ArrayList<Integer> getAllSortedTasks(String component){
		ArrayList<Integer> result = new ArrayList<>();
		Map<ExecutorDetails, String> executorToComponents = this.topology.getExecutorToComponent();
		for(ExecutorDetails executor : executorToComponents.keySet()){
			if(executorToComponents.get(executor).equalsIgnoreCase(component)){
				int start = executor.getStartTask();
				int stop = executor.getEndTask();
				for(int i = start; i <= stop; i++){
					result.add(i);
				}
			}
		}
		Collections.sort(result);
		return result;
	}
	
	public ArrayList<ExecutorDetails> getAllExecutors(String component){
		ArrayList<ExecutorDetails> result = new ArrayList<>();
		Map<ExecutorDetails, String> executorToComponents = this.topology.getExecutorToComponent();
		for(ExecutorDetails executor : executorToComponents.keySet()){
			if(executorToComponents.get(executor).equalsIgnoreCase(component)){
				result.add(executor);
			}
		}
		return result;
	}
	
	public Integer getParallelism(String component){
		int result = 0;
		Map<ExecutorDetails, String> executorToComponents = this.topology.getExecutorToComponent();
		for(ExecutorDetails executor : executorToComponents.keySet()){
			if(executorToComponents.get(executor).equalsIgnoreCase(component)){
				result++;
			}
		}
		return result;
	}
}
