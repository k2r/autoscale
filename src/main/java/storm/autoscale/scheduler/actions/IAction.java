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
	
	public HashMap<String, WorkerSlot> getBestLocation();
}
