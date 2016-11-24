/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public interface IMetric {
	
	public ComponentMonitor getComponentMonitor();
	
	public TopologyExplorer getTopologyExplorer();
	
	public Double compute(String component);

}
