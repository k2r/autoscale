/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;

/**
 * @author Roland
 *
 */
public class AutoscaleScheduler implements IScheduler {

	/**
	 * 
	 */
	public AutoscaleScheduler() {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see backtype.storm.scheduler.IScheduler#prepare(java.util.Map)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see backtype.storm.scheduler.IScheduler#schedule(backtype.storm.scheduler.Topologies, backtype.storm.scheduler.Cluster)
	 */
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		// TODO Auto-generated method stub

	}

}
