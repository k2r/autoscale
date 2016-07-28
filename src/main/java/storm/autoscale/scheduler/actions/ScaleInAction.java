/**
 * 
 */
package storm.autoscale.scheduler.actions;

import java.util.HashMap;

import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import storm.autoscale.scheduler.allocation.DelegatedAllocationStrategy;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

/**
 * @author Roland
 *
 */
public class ScaleInAction implements IAction {

	/**
	 * 
	 */
	public ScaleInAction(HashMap<String, ComponentWindowedStats> needScaleInStats, TopologyDetails topology,
			AssignmentMonitor assignMonitor, DelegatedAllocationStrategy delegatedAllocationStrategy, String nimbusHost,
			Integer nimbusPort) {
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
