/**
 * 
 */
package storm.autoscale.scheduler.actions;

import java.util.HashMap;

import backtype.storm.scheduler.WorkerSlot;

/**
 * @author Roland
 *
 */
public interface IAction extends Runnable{
	
	public void validate();
	
	public HashMap<String, WorkerSlot> getBestLocation();
}
