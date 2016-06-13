/**
 * 
 */
package storm.autoscale.scheduler.actions;

import backtype.storm.scheduler.WorkerSlot;

/**
 * @author Roland
 *
 */
public interface IAction {

	public void setParallelism();
	
	public WorkerSlot getBestLocation();
	
	public void unassign();
	
	public void scale();
}
