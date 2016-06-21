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
	
	public WorkerSlot getBestLocation();
}
