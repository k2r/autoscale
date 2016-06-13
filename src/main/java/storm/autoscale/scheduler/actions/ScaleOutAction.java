/**
 * 
 */
package storm.autoscale.scheduler.actions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import storm.autoscale.scheduler.allocation.IAllocationStrategy;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentStats;

/**
 * @author Roland
 *
 */
public class ScaleOutAction implements IAction {

	private String component;
	private TopologyDetails topology;
	private Cluster cluster;
	private ComponentMonitor compMonitor;
	private AssignmentMonitor assignMonitor;
	private IAllocationStrategy allocStrategy;
	
	public ScaleOutAction(String component, TopologyDetails topology, Cluster cluster, ComponentMonitor cm, AssignmentMonitor am, IAllocationStrategy as) {
		this.component = component;
		this.topology = topology;
		this.cluster = cluster;
		this.compMonitor = cm;
		this.assignMonitor = am;
		this.allocStrategy = as;
	}
	
	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.actions.IAction#setParallelism()
	 */
	@Override
	public void setParallelism() {
		int parallelism = this.assignMonitor.getParallelism(component);
		Double paralFactor = (parallelism + 1) / (parallelism * 1.0);
		ComponentStats stats = this.compMonitor.getStats(component);
		Double expectedExecuted = Math.min(stats.getNbExecuted() * paralFactor, stats.getNbInputs());
		stats.setNbExecuted(expectedExecuted);
		this.compMonitor.updateStatistics(stats);
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.actions.IAction#getBestLocation()
	 */
	@Override
	public WorkerSlot getBestLocation() {
		WorkerSlot result = null;
		Double bestScore = Double.NEGATIVE_INFINITY;
		HashMap<WorkerSlot, Double> scores = this.allocStrategy.getScores(this.component);
		for(WorkerSlot ws : scores.keySet()){
			Double score = scores.get(ws);
			if(score > bestScore){
				result = ws;
				bestScore = score;
			}
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.actions.IAction#unassign()
	 */
	@Override
	public void unassign() {
		SchedulerAssignment schedAssignment = this.cluster.getAssignmentById(this.topology.getId());
		Set<ExecutorDetails> executors = schedAssignment.getExecutors(); 
		Map<ExecutorDetails, String> executorToComponents = this.topology.getExecutorToComponent();
		Iterator<ExecutorDetails> iterator = executors.iterator();
		while(iterator.hasNext()){
			ExecutorDetails executor = iterator.next();
			String linkedComponent = executorToComponents.get(executor);
			if(linkedComponent.equalsIgnoreCase(this.component) && schedAssignment.isExecutorAssigned(executor)){
				WorkerSlot slot = schedAssignment.getExecutorToSlot().get(executor);
				this.cluster.freeSlot(slot);
			}
		}
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.actions.IAction#scale()
	 */
	@Override
	public void scale() {
		ArrayList<ExecutorDetails> executors = new ArrayList<>();
		this.unassign();
		ComponentStats stats = this.compMonitor.getStats(this.component);
		/*Add of enough Executors to process all incoming tuples at an equivalent input and process rate*/
		int nbExecToAdd = (int) Math.round((stats.getNbInputs() - stats.getNbExecuted()) / stats.getNbExecuted()); 
		ArrayList<Integer> tasks = this.assignMonitor.getAllSortedTasks(component);
		/*Take into account that we can not create more Executors than there are tasks*/
		int newParallelism = Math.max(tasks.size(), this.assignMonitor.getParallelism(component) + nbExecToAdd);
		ArrayList<ArrayList<Integer>> borders = UtilFunctions.getBuckets(tasks, newParallelism);
		int nbExecutors = borders.size();
		for(int i = 0; i < nbExecutors; i++){
			ArrayList<Integer> executorBorders = borders.get(i);
			int start = executorBorders.get(0);
			int end = executorBorders.get(1);
			ExecutorDetails executor = new ExecutorDetails(start, end);
			executors.add(executor);
		}
		for(int i = 0; i < executors.size(); i++){
			ExecutorDetails executor = executors.get(i);
			WorkerSlot slot = this.getBestLocation();
			if(this.cluster.isSlotOccupied(slot)){
				ArrayList<ExecutorDetails> sharedExecutorPool = new ArrayList<>();
				sharedExecutorPool.add(executor);
				this.cluster.assign(slot, this.topology.getId(), sharedExecutorPool);
			}else{
				ArrayList<ExecutorDetails> exclusiveExecutorPool = new ArrayList<>();
				exclusiveExecutorPool.add(executor);
				this.cluster.assign(slot, this.topology.getId(), exclusiveExecutorPool);
			}
		}
	}
}