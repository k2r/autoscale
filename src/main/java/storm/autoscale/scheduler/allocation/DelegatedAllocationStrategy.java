/**
 * 
 */
package storm.autoscale.scheduler.allocation;

import java.util.HashMap;

import org.apache.storm.scheduler.WorkerSlot;

import storm.autoscale.scheduler.modules.assignment.AssignmentMonitor;

/**
 * @author Roland
 *
 */
public class DelegatedAllocationStrategy implements IAllocationStrategy {

	private AssignmentMonitor assignMonitor;
	
	public DelegatedAllocationStrategy(AssignmentMonitor am) {
		this.assignMonitor = am;
	}
	
	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.allocation.IAllocationStrategy#computeScore(java.lang.String, backtype.storm.scheduler.WorkerSlot)
	 */
	@Override
	public Double computeScore(String component, WorkerSlot worker) {
		return Double.NEGATIVE_INFINITY;
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