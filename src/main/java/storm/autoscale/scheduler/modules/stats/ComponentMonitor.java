/**
 * 
 */
package storm.autoscale.scheduler.modules.stats;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class ComponentMonitor {
	
	private HashMap<String, ComponentWindowedStats> stats;
	private StatStorageManager manager;
	private static Logger logger = Logger.getLogger("ComponentMonitor");
	
	/**
	 * 
	 */
	public ComponentMonitor(String dbHost, String nimbusHost, Integer nimbusPort, Integer rate) {
		this.stats = new HashMap<>();
		try {
			this.manager = StatStorageManager.getManager(dbHost, nimbusHost, nimbusPort, rate);
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void getStatistics(TopologyExplorer explorer){
		this.reset();
		Integer current = this.manager.getCurrentTimestamp();
		Integer previous = this.manager.getStoredTimestamp(); 
		Set<String> spouts = explorer.getSpouts();
		for(String spout : spouts){
			Double nbInputs = 0.0;
			Double nbExecuted = 0.0;
			Double nbOutputs = this.manager.getSpoutOutputs(spout, current) - this.manager.getSpoutOutputs(spout, previous) * 1.0;
			Double avgTopLatency = this.manager.getTopologyAvgLatency(explorer.getTopologyName(), current) * 1.0;
			ComponentWindowedStats component = new ComponentWindowedStats(spout, nbInputs, nbExecuted, nbOutputs, avgTopLatency);
			this.stats.put(spout, component);
		}
		Set<String> bolts = explorer.getBolts();
		for(String bolt : bolts){
			Double nbInputs = 0.0;
			ArrayList<String> parents = explorer.getParents(bolt);
			for(String parent : parents){
				Long parentOutput = this.manager.getBoltOutputs(parent, current);
				Long spoutParentOutputs = this.manager.getSpoutOutputs(parent, previous);
				Long boltParentOutputs = this.manager.getBoltOutputs(parent, previous);
				if(spoutParentOutputs > 0){
					parentOutput = parentOutput - spoutParentOutputs;
				}else{
					if(boltParentOutputs > 0){
						parentOutput = parentOutput - boltParentOutputs;
					}
				}
				nbInputs += parentOutput;
			}
			Double nbExecuted = this.manager.getExecuted(bolt, current) - this.manager.getExecuted(bolt, previous) * 1.0;
			Double nbOutputs = this.manager.getBoltOutputs(bolt, current) - this.manager.getBoltOutputs(bolt, previous) * 1.0;
			Double avgLatency = this.manager.getAvgLatency(bolt, current);
			ComponentWindowedStats component = new ComponentWindowedStats(bolt, nbInputs, nbExecuted, nbOutputs, avgLatency);
			this.stats.put(bolt, component);
		}
	}
	
	public Set<String> getRegisteredComponents(){
		return this.stats.keySet();
	}
	
	public ComponentWindowedStats getStats(String component){
		return this.stats.get(component);
	}
	
	public void updateStatistics(ComponentWindowedStats stats){
		if(this.stats.containsKey(stats.getId())){
			this.stats.replace(stats.getId(), stats);
		}else{
			this.stats.put(stats.getId(), stats);
		}
	}
	
	public boolean isCongested(String component){
		if(this.stats.containsKey(component)){
			ComponentWindowedStats componentWindowedStats = this.stats.get(component);
			if(componentWindowedStats.getNbInputs() > componentWindowedStats.getNbExecuted()){
				return true;
			}else{
				return false;
			}
		}else{
			logger.warning("Looking for an non-existing component " + component);
			return false;
		}
	}
	
	public boolean couldCongest(String component){
		//TODO Anticipate a congestion thanks to the condition: CPU usage of supervisor > threshold & inputs growing up
		return false;
	}
	
	public ArrayList<String> getCongested(){
		ArrayList<String> result = new ArrayList<>();
		for(String component : this.stats.keySet()){
			if(isCongested(component)){
				result.add(component);
			}
		}
		return result;
	}
	
	public ArrayList<String> getPotentialCongested(){
		ArrayList<String> result = new ArrayList<>();
		for(String component : this.stats.keySet()){
			if(couldCongest(component)){
				result.add(component);
			}
		}
		return result;
	}
	
	public void reset(){
		this.stats = new HashMap<>();
	}
}