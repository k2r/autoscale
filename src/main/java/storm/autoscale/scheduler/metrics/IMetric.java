/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public interface IMetric {
	
	public TopologyExplorer getTopologyExplorer();
	
	public ComponentMonitor getComponentMonitor();
	
	public Double compute(String component);

}
