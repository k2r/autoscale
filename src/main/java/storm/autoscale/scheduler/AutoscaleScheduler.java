/**
 * 
 */
package storm.autoscale.scheduler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.DefaultScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
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
	private ArrayList<String> toFork;
	private ArrayList<ExecutorDetails> toAssign;
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
	
	public void forkAndAssign(String component, Cluster cluster, TopologyDetails topology){
		int newParallelism = this.assignments.getParallelism(component) + 1;
		ArrayList<Integer> tasks = this.assignments.getAllSortedTasks(component);
		int nbTasks = tasks.size();
		int quotient = nbTasks / newParallelism;
		int remainder = nbTasks - (newParallelism * quotient);
		if(remainder == 0){
			for(int i = 0; i < nbTasks; i += quotient){
				int start = tasks.get(i);
				int end = tasks.get(i + quotient - 1);
				ExecutorDetails executor = new ExecutorDetails(start, end);
				this.toAssign.add(executor);
			}
			this.toFork.remove(component);
		}else{
			for(int i = 0; i < nbTasks - remainder; i += quotient){
				int start = tasks.get(i);
				int end = tasks.get(i + quotient - 1);
				ExecutorDetails executor = new ExecutorDetails(start, end);
				this.toAssign.add(executor);
			}
			int start = tasks.get(nbTasks - remainder);
			int end = tasks.get(nbTasks - 1);
			ExecutorDetails executor = new ExecutorDetails(start, end);
			this.toAssign.add(executor);
			this.toFork.remove(component);
		}
		for(ExecutorDetails executor : this.toAssign){
			WorkerSlot slot = getBestLocation(component);
			if(cluster.isSlotOccupied(slot)){
				ArrayList<ExecutorDetails> sharedExecutorPool = new ArrayList<>();
				sharedExecutorPool.add(executor);
				cluster.freeSlot(slot);
				sharedExecutorPool.addAll(cluster.getUnassignedExecutors(topology));
				cluster.assign(slot, topology.getId(), sharedExecutorPool);
				this.toAssign.remove(executor);
			}else{
				ArrayList<ExecutorDetails> exclusiveExecutorPool = new ArrayList<>();
				exclusiveExecutorPool.add(executor);
				cluster.assign(slot, topology.getId(), exclusiveExecutorPool);
				this.toAssign.remove(executor);
			}
		}
		logger.info("Component " + component + " has been forked");
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.scheduler.IScheduler#schedule(backtype.storm.scheduler.Topologies, backtype.storm.scheduler.Cluster)
	 */
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		/*In a first time, we let the default scheduler balance the load*/
		DefaultScheduler scheduler = new DefaultScheduler();
		scheduler.schedule(topologies, cluster);
		
		/*Then, we take all scaling decisions*/
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
			this.toFork = new ArrayList<>();
			this.toAssign = new ArrayList<>();
			
			while(!this.congested.isEmpty()){
				System.out.println(this.congested.size() + " congested components");
				String mostImportantComponent = this.congested.get(0);
				for(String component : this.congested){
					if(impactMetric.compute(component) > impactMetric.compute(mostImportantComponent)){
						mostImportantComponent = component;
					}
				}
				this.toFork.add(mostImportantComponent);
				incrParallelism(mostImportantComponent);
				if(!this.components.isCongested(mostImportantComponent)){
					this.congested.remove(mostImportantComponent);
				}
			}
			for(String component : this.toFork){
				forkAndAssign(component, cluster, topology);
			}	
		}
	}
}
