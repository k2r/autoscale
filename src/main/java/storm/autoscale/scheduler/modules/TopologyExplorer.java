/**
 * 
 */
package storm.autoscale.scheduler.modules;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;

/**
 * @author Roland
 *
 */
public class TopologyExplorer {

	private String name;
	private StormTopology topology;
	private Map<String, SpoutSpec> spouts;
	private Map<String, Bolt> bolts;
	private ArrayList<String> components;
	
	/**
	 * 
	 */
	public TopologyExplorer(String name, StormTopology topology) {
		this.name = name;
		this.topology = topology;
		this.spouts = this.topology.get_spouts();
		this.bolts = this.topology.get_bolts();
		this.components = new ArrayList<>();
		this.components.addAll(this.spouts.keySet());
		this.components.addAll(this.bolts.keySet());
	}
	
	public String getTopologyName(){
		return this.name;
	}
	
	public ArrayList<String> getComponents(){
		return this.components;
	}
	 
	public Set<String> getSpouts(){
		return this.spouts.keySet();
	}
	
	public Set<String> getBolts(){
		return this.bolts.keySet();
	}

	public boolean areLinked(String component1, String component2){
		boolean result = false;
		if(this.spouts.containsKey(component1)){
			ComponentCommon component = this.spouts.get(component1).get_common();
			for(GlobalStreamId input : component.get_inputs().keySet()){
				if(input.get_componentId().equalsIgnoreCase(component2)){
					result = true;
					break;
				}
			}
		}
		if(this.bolts.containsKey(component1)){
			ComponentCommon component = this.bolts.get(component1).get_common();
			for(GlobalStreamId input : component.get_inputs().keySet()){
				if(input.get_componentId().equalsIgnoreCase(component2)){
					result = true;
					break;
				}
			}
		}
		if(this.spouts.containsKey(component2)){
			ComponentCommon component = this.spouts.get(component2).get_common();
			for(GlobalStreamId input : component.get_inputs().keySet()){
				if(input.get_componentId().equalsIgnoreCase(component1)){
					result = true;
					break;
				}
			}
		}
		if(this.bolts.containsKey(component2)){
			ComponentCommon component = this.bolts.get(component2).get_common();
			for(GlobalStreamId input : component.get_inputs().keySet()){
				if(input.get_componentId().equalsIgnoreCase(component1)){
					result = true;
					break;
				}
			}
		}
		return result;
	}
	
	public ArrayList<String> getChildren(String parent){
		ArrayList<String> result = new ArrayList<>();
		for(String candidate : this.components){
			if(this.spouts.containsKey(candidate)){
				ComponentCommon component = this.spouts.get(candidate).get_common();
				for(GlobalStreamId input : component.get_inputs().keySet()){
					if(input.get_componentId().equalsIgnoreCase(parent)){
						result.add(candidate);
						break;
					}
				}
			}
			if(this.bolts.containsKey(candidate)){
				ComponentCommon component = this.bolts.get(candidate).get_common();
				for(GlobalStreamId input : component.get_inputs().keySet()){
					if(input.get_componentId().equalsIgnoreCase(parent)){
						result.add(candidate);
						break;
					}
				}
			}
		}
		return result;
	}
	
	public ArrayList<String> getParents(String child){
		ArrayList<String> result = new ArrayList<>();
		if(this.spouts.containsKey(child)){
			ComponentCommon component = this.spouts.get(child).get_common();
			for(GlobalStreamId input : component.get_inputs().keySet()){
				result.add(input.get_componentId());
			}
		}
		if(this.bolts.containsKey(child)){
			ComponentCommon component = this.bolts.get(child).get_common();
			for(GlobalStreamId input : component.get_inputs().keySet()){
				result.add(input.get_componentId());
			}
		}
		return result;
	}
}