/**
 * 
 */
package storm.autoscale.scheduler.actions;

import backtype.storm.scheduler.WorkerSlot;

/**
 * @author Roland
 *
 */
public interface IAction extends Runnable{

	public void setParallelism();
	
	public WorkerSlot getBestLocation();
	
	public void unassign();

	void scale();
}
