/**
 * 
 */
package storm.autoscale.scheduler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import storm.autoscale.scheduler.metrics.IMetric;
import storm.autoscale.scheduler.metrics.WelfMetric;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class AutoscaleScheduler implements IScheduler {
	
	private ComponentMonitor components;
	private AssignmentMonitor assignments;
	private TopologyExplorer explorer;
	private ArrayList<String> congested;
	//private ArrayList<String> toFork;
	private ArrayList<ExecutorDetails> toAssign;
	private boolean isScaled;
	private static Logger logger = Logger.getLogger("AutoscaleScheduler");

	/**
	 * 
	 */
	public AutoscaleScheduler() {
		logger.info("The auto-scaling scheduler for Storm is starting...");
	}

	/* (non-Javadoc)
	 * @see backtype.storm.scheduler.IScheduler#prepare(java.util.Map)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf) {
	}


	public void incrParallelism(String component){
		int parallelism = this.assignments.getParallelism(component);
		Double paralFactor = (parallelism + 1) / (parallelism * 1.0);
		Double expectedExecuted = Math.min(this.components.getNbExecuted(component) * paralFactor, this.components.getInputQueueSize(component));
		this.components.setNbExecuted(component, expectedExecuted);
	}
	
	public WorkerSlot getBestLocation(String component){
		/*First we look on a free slot*/
		WorkerSlot best = null;
		ArrayList<WorkerSlot> freeSlots = this.assignments.getFreeSlots();
		if(!freeSlots.isEmpty()){
			best = freeSlots.get(0);
			for(WorkerSlot slot : freeSlots){	
				if(this.assignments.getProximity(slot.getNodeId(), component) > this.assignments.getProximity(best.getNodeId(), component)){
					best = slot;
				}
			}
		}else{
			ArrayList<WorkerSlot> sharableSlots = this.assignments.getSharableSlots(component);
			if(!sharableSlots.isEmpty()){
				best = sharableSlots.get(0);
				for(WorkerSlot slot : sharableSlots){	
					if(this.assignments.getProximity(slot.getNodeId(), component) > this.assignments.getProximity(best.getNodeId(), component)){
						best = slot;
					}
				}
			}
		}
		return best;
	}
	
	public void unassign(String component, Cluster cluster, TopologyDetails topology){
		SchedulerAssignment schedAssignment = cluster.getAssignmentById(topology.getId());
		Set<ExecutorDetails> executors = schedAssignment.getExecutors(); 
		Map<ExecutorDetails, String> executorToComponents = topology.getExecutorToComponent();
		Iterator<ExecutorDetails> iterator = executors.iterator();
		while(iterator.hasNext()){
			ExecutorDetails executor = iterator.next();
			String linkedComponent = executorToComponents.get(executor);
			if(linkedComponent.equalsIgnoreCase(component) && schedAssignment.isExecutorAssigned(executor)){
				WorkerSlot slot = schedAssignment.getExecutorToSlot().get(executor);
				cluster.freeSlot(slot);
			}
		}
	}
	
	public ArrayList<ArrayList<Integer>> getBorderTasks(ArrayList<Integer> tasks, Integer parallelism){
		ArrayList<ArrayList<Integer>> result = new ArrayList<>();
		int nbTasks = tasks.size();
		int quotient = nbTasks / parallelism;
		int remainder = nbTasks - (parallelism * quotient);
		if(remainder == 0){
			for(int i = 0; i < nbTasks; i += quotient){
				ArrayList<Integer> delimiters = new ArrayList<>();
				int start = tasks.get(i);
				int end = tasks.get(i + quotient - 1);
				delimiters.add(start);
				delimiters.add(end);
				result.add(delimiters);
			}
		}else{
			for(int i = 0; i < nbTasks - (quotient + remainder); i += quotient){
				ArrayList<Integer> delimiters = new ArrayList<>();
				int start = tasks.get(i);
				int end = tasks.get(i + quotient - 1);
				delimiters.add(start);
				delimiters.add(end);
				result.add(delimiters);
			}
			ArrayList<Integer> delimiters = new ArrayList<>();
			int start = tasks.get(nbTasks - (quotient + remainder));
			int end = tasks.get(nbTasks - 1);
			delimiters.add(start);
			delimiters.add(end);
			result.add(delimiters);
		}
		return result;
	}
	
	public void forkAndAssign(String component, Cluster cluster, TopologyDetails topology){
		unassign(component, cluster, topology);
		int newParallelism = this.assignments.getParallelism(component) + 1;
		ArrayList<Integer> tasks = this.assignments.getAllSortedTasks(component);
		ArrayList<ArrayList<Integer>> borders = getBorderTasks(tasks, newParallelism);
		int nbExecutors = borders.size();
		for(int i = 0; i < nbExecutors; i++){
			ArrayList<Integer> executorBorders = borders.get(i);
			int start = executorBorders.get(0);
			int end = executorBorders.get(1);
			ExecutorDetails executor = new ExecutorDetails(start, end);
			this.toAssign.add(executor);
		}
		for(int i = 0; i < this.toAssign.size(); i++){
			ExecutorDetails executor = this.toAssign.get(i);
			WorkerSlot slot = getBestLocation(component);
			if(cluster.isSlotOccupied(slot)){
				ArrayList<ExecutorDetails> sharedExecutorPool = new ArrayList<>();
				sharedExecutorPool.add(executor);
				//cluster.freeSlot(slot);
				//sharedExecutorPool.addAll(cluster.getUnassignedExecutors(topology));
				cluster.assign(slot, topology.getId(), sharedExecutorPool);
				//this.toAssign.remove(executor);
			}else{
				ArrayList<ExecutorDetails> exclusiveExecutorPool = new ArrayList<>();
				exclusiveExecutorPool.add(executor);
				cluster.assign(slot, topology.getId(), exclusiveExecutorPool);
				//this.toAssign.remove(executor);
			}
		}
		logger.info("Component " + component + " has been uncongested successfully");
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.scheduler.IScheduler#schedule(backtype.storm.scheduler.Topologies, backtype.storm.scheduler.Cluster)
	 */
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		/*In a first time, we take all scaling decisions*/
		for(TopologyDetails topology : topologies.getTopologies()){
			
			try {
				this.components = new ComponentMonitor("localhost");
			} catch (ClassNotFoundException e) {
				logger.severe("The autoscale scheduler can not be launched because of its component monitor, error while starting: " + e);
			} catch (SQLException e) {
				logger.severe("The autoscale scheduler can not be launched because of its component monitor, error while starting: " + e);
			}
			this.assignments = new AssignmentMonitor(cluster, topology);
			this.explorer = new TopologyExplorer(topology.getTopology());
			this.assignments.update();
			this.components.update();
			IMetric impactMetric = new WelfMetric(this.explorer, this.components);
			
			this.congested = this.components.getCongested();
			//this.toFork = new ArrayList<>();
			this.toAssign = new ArrayList<>();
			
			String monitoring = "Current monitoring info (timestamp " + this.components.getLastTimestamp() + " to " + this.components.getCurrentTimestamp() + ")\n";
			logger.info(monitoring);
			for(String component : this.components.getComponents()){
				String infos = "Component " + component + " ---> inputs: " + this.components.getInputQueueSize(component);
				infos += ", executed: " + this.components.getNbExecuted(component);
				infos += ", outputs: " + this.components.getNbOutputs(component); 
				infos += ", latency: " + this.components.getAvgLatency(component) + "\n";
				logger.info(infos);
			}
			if(this.congested.isEmpty()){
				this.isScaled = true;
				logger.info("No component to scale out!");
			}else{
				this.isScaled = false;
				String congestInfo = "Congested components ";
				for(String component : this.congested){
					congestInfo += component + " ";
				}
				congestInfo += "have been detected!";
				logger.info(congestInfo);
			}
			while(!isScaled){
				if(this.congested.isEmpty()){
					this.isScaled = true;
					break;
				}else{
					String mostImportantComponent = this.congested.get(0);
					for(String component : this.congested){
						if(impactMetric.compute(component) > impactMetric.compute(mostImportantComponent)){
							mostImportantComponent = component;
						}
					}
					//this.toFork.add(mostImportantComponent);
					incrParallelism(mostImportantComponent);
					this.congested.remove(mostImportantComponent);
					logger.info("Component " + mostImportantComponent + " is being uncongested...");
					forkAndAssign(mostImportantComponent, cluster, topology);
				}
			}
				
		}
		
		/*Then we let the default scheduler balance the load*/
		EvenScheduler scheduler = new EvenScheduler();
		scheduler.schedule(topologies, cluster);
	}
}