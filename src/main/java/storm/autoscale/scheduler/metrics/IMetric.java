/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import storm.autoscale.scheduler.modules.ComponentMonitor;

/**
 * @author Roland
 *
 */
public interface IMetric {
	
	public ComponentMonitor getComponentMonitor();
	
	public Double compute(String component);

}
