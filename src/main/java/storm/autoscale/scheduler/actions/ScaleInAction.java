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
public class ScaleInAction implements IAction {

	/**
	 * 
	 */
	public ScaleInAction() {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.actions.IAction#getBestLocation()
	 */
	@Override
	public HashMap<String, WorkerSlot> getBestLocation() {
		// TODO Auto-generated method stub
		return null;
	}

}
