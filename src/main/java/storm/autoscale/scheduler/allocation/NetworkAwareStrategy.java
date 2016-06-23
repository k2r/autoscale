/**
 * 
 */
package storm.autoscale.scheduler.allocation;

import java.util.ArrayList;
import java.util.HashMap;

import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

/**
 * @author Roland
 *
 */
public class NetworkAwareStrategy implements IAllocationStrategy {

	private TopologyExplorer explorer;
	private ComponentMonitor compMonitor;
	private AssignmentMonitor assignMonitor;
	
	public NetworkAwareStrategy(TopologyExplorer explorer, ComponentMonitor cm, AssignmentMonitor am) {
		this.explorer = explorer;
		this.compMonitor = cm;
		this.assignMonitor = am;
	}
	
	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.allocation.IAllocationStrategy#computeScore(java.lang.String, backtype.storm.scheduler.WorkerSlot)
	 */
	@Override
	public Double computeScore(String component, WorkerSlot worker) {
		Double result = 0.0;
		HashMap<SupervisorDetails, ArrayList<WorkerSlot>> supToWorkers = this.assignMonitor.getWorkers();
		SupervisorDetails supervisor = this.assignMonitor.getSupervisor(worker, supToWorkers);
		ArrayList<WorkerSlot> neighbours = supToWorkers.get(supervisor);
		neighbours.remove(worker);//we do not need to count the candidate in his neighborhood
		for(WorkerSlot neighbour : neighbours){
			ArrayList<String> neighbourComponents = this.assignMonitor.getRunningComponents(neighbour);
			for(String neighbourComp : neighbourComponents){
				if(this.explorer.areLinked(component, neighbourComp)){
					if(this.explorer.getParents(component).contains(neighbourComp)){
						ComponentWindowedStats stats = this.compMonitor.getStats(neighbourComp);
						result += ComponentWindowedStats.getLastRecord(stats.getOutputsRecords()) / this.assignMonitor.getParallelism(neighbourComp);
					}else{
						ComponentWindowedStats stats = this.compMonitor.getStats(component);
						result += ComponentWindowedStats.getLastRecord(stats.getOutputsRecords()) / this.assignMonitor.getParallelism(component);
					}
				}
			}
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.allocation.IAllocationStrategy#getScores(java.lang.String)
	 */
	@Override
	public HashMap<WorkerSlot, Double> getScores(String component) {
		HashMap<WorkerSlot, Double> result = new HashMap<>();
		for(WorkerSlot slot : this.assignMonitor.getFreeSlots()){
			Double score = this.computeScore(component, slot);
			result.put(slot, score);
		}
		return result;
	}
}