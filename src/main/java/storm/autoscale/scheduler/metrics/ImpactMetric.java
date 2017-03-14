/**
 * 
 */
package storm.autoscale.scheduler.metrics;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;

import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.ScalingManager;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

/**
 * @author Roland
 *
 */
public class ImpactMetric implements IMetric {

	ScalingManager sm;
	TopologyExplorer explorer;
	HashMap<String, Integer> impactDegrees;
	
	public ImpactMetric(ScalingManager sm, TopologyExplorer explorer) {
		this.sm = sm;
		this.explorer = explorer;
		this.impactDegrees = new HashMap<>();
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#getComponentMonitor()
	 */
	@Override
	public ComponentMonitor getComponentMonitor() {
		return this.sm.getMonitor();
	}
	
	@Override
	public TopologyExplorer getTopologyExplorer() {
		return this.explorer;
	}
	
	/**
	 * @return the impactDegrees of registered components
	 */
	public HashMap<String, Integer> getImpactDegrees() {
		return impactDegrees;
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.metrics.IMetric#compute(java.lang.String)
	 */
	@Override
	public Double compute(String component) {
		ComponentMonitor cm = this.getComponentMonitor();
		Double result = 0.0;// we return the value Impact(*,component)
		ArrayList<String> parents = this.explorer.getParents(component);
		for(String parent : parents){
			Double estimatedLoad = this.sm.getEstimatedLoad(parent);
			Double selectivity = ComponentWindowedStats.getLastRecord(cm.getStats(parent).getSelectivityRecords());
			Double impact = estimatedLoad * selectivity;
			result += impact;
		}
		
		Double capacityPerWindow = this.sm.getCapacity(component) * this.sm.getParser().getWindowSize();
		Double gal = result / capacityPerWindow;
		if(gal.isInfinite() ||gal.isNaN()){
			gal = 0.0;
		}
		int impactDegree = new BigDecimal(this.sm.getDegree(component) * gal).setScale(0, BigDecimal.ROUND_UP).intValue();
		impactDegree = Math.max(1, impactDegree);//at least one excutor must remain
		this.impactDegrees.put(component, impactDegree);
		return result;
	}
}