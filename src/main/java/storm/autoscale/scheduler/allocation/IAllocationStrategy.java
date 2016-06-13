/**
 * 
 */
package storm.autoscale.scheduler.allocation;

import java.util.HashMap;

import backtype.storm.scheduler.WorkerSlot;

/**
 * @author Roland
 *
 */
public interface IAllocationStrategy {

	public Double computeScore(String component, WorkerSlot worker);
	
	public HashMap<WorkerSlot, Double> getScores(String component);
}
