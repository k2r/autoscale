/**
 * 
 */
package storm.autoscale.scheduler.modules;

import java.util.ArrayList;
import java.util.Map;

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

	private StormTopology topology;
	private Map<String, SpoutSpec> spouts;
	private Map<String, Bolt> bolts;
	private ArrayList<String> components;
	
	/**
	 * 
	 */
	public TopologyExplorer(StormTopology topology) {
		this.topology = topology;
		this.spouts = this.topology.get_spouts();
		this.bolts = this.topology.get_bolts();
		this.components = new ArrayList<>();
		this.components.addAll(this.spouts.keySet());
		this.components.addAll(this.bolts.keySet());
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
}
