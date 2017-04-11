/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import org.mockito.Mockito;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import junit.framework.TestCase;
import storm.autoscale.scheduler.modules.explorer.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class TopologyExplorerTest extends TestCase {

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.explorer.TopologyExplorer#areLinked(java.lang.String, java.lang.String)}.
	 */
	public final void testAreLinked() {
		GlobalStreamId gsA = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsA.get_componentId()).thenReturn("A");
		GlobalStreamId gsC = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsC.get_componentId()).thenReturn("C");
		GlobalStreamId gsD = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsD.get_componentId()).thenReturn("D");
		GlobalStreamId gsF = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsF.get_componentId()).thenReturn("F");
		Grouping grouping = Mockito.mock(Grouping.class);
		
		HashMap<GlobalStreamId, Grouping> inputsA = new HashMap<>();
		
		HashMap<GlobalStreamId, Grouping> inputsC = new HashMap<>();
		inputsC.put(gsA, grouping);
		
		HashMap<GlobalStreamId, Grouping> inputsD = new HashMap<>();
		inputsD.put(gsC, grouping);
		
		HashMap<GlobalStreamId, Grouping> inputsF = new HashMap<>();
		inputsF.put(gsC, grouping);
		
		ComponentCommon ccA = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccA.get_inputs()).thenReturn(inputsA);
		
		ComponentCommon ccC = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccC.get_inputs()).thenReturn(inputsC);
		
		ComponentCommon ccD = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccD.get_inputs()).thenReturn(inputsD);
		
		ComponentCommon ccF = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccF.get_inputs()).thenReturn(inputsF);
		
		SpoutSpec spoutA = Mockito.mock(SpoutSpec.class);
		Mockito.when(spoutA.get_common()).thenReturn(ccA);
		
		Bolt boltC = Mockito.mock(Bolt.class);
		Mockito.when(boltC.get_common()).thenReturn(ccC);
		
		Bolt boltD = Mockito.mock(Bolt.class);
		Mockito.when(boltD.get_common()).thenReturn(ccD);
		
		Bolt boltF = Mockito.mock(Bolt.class);
		Mockito.when(boltF.get_common()).thenReturn(ccF);
		
		HashMap<String, SpoutSpec> spouts = new HashMap<>();
		spouts.put("A", spoutA);
		
		HashMap<String, Bolt> bolts = new HashMap<>();
		bolts.put("C", boltC);
		bolts.put("D", boltD);
		bolts.put("F", boltF);
		
		StormTopology topology = Mockito.mock(StormTopology.class);
		Mockito.when(topology.get_bolts()).thenReturn(bolts);
		Mockito.when(topology.get_spouts()).thenReturn(spouts);
		
		TopologyExplorer explorer = new TopologyExplorer("test", topology);
		boolean link1 = explorer.areLinked("A", "C");
		boolean link2 = explorer.areLinked("C", "A");
		boolean link3 = explorer.areLinked("C", "D");
		boolean link4 = explorer.areLinked("F", "C");
		boolean link5 = explorer.areLinked("A", "F");
		boolean link6 = explorer.areLinked("F", "A");
		
		assertEquals(true, link1);
		assertEquals(true, link2);
		assertEquals(true, link3);
		assertEquals(true, link4);
		assertEquals(false, link5);
		assertEquals(false, link6);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.explorer.TopologyExplorer#getChildren(java.lang.String)}.
	 */
	public final void testGetChildren() {
		GlobalStreamId gsA = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsA.get_componentId()).thenReturn("A");
		GlobalStreamId gsC = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsC.get_componentId()).thenReturn("C");
		GlobalStreamId gsD = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsD.get_componentId()).thenReturn("D");
		GlobalStreamId gsF = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsF.get_componentId()).thenReturn("F");
		Grouping grouping = Mockito.mock(Grouping.class);
		
		HashMap<GlobalStreamId, Grouping> inputsA = new HashMap<>();
		
		HashMap<GlobalStreamId, Grouping> inputsC = new HashMap<>();
		inputsC.put(gsA, grouping);
		
		HashMap<GlobalStreamId, Grouping> inputsD = new HashMap<>();
		inputsD.put(gsC, grouping);
		
		HashMap<GlobalStreamId, Grouping> inputsF = new HashMap<>();
		inputsF.put(gsC, grouping);
		
		ComponentCommon ccA = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccA.get_inputs()).thenReturn(inputsA);
		
		ComponentCommon ccC = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccC.get_inputs()).thenReturn(inputsC);
		
		ComponentCommon ccD = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccD.get_inputs()).thenReturn(inputsD);
		
		ComponentCommon ccF = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccF.get_inputs()).thenReturn(inputsF);
		
		SpoutSpec spoutA = Mockito.mock(SpoutSpec.class);
		Mockito.when(spoutA.get_common()).thenReturn(ccA);
		
		Bolt boltC = Mockito.mock(Bolt.class);
		Mockito.when(boltC.get_common()).thenReturn(ccC);
		
		Bolt boltD = Mockito.mock(Bolt.class);
		Mockito.when(boltD.get_common()).thenReturn(ccD);
		
		Bolt boltF = Mockito.mock(Bolt.class);
		Mockito.when(boltF.get_common()).thenReturn(ccF);
		
		HashMap<String, SpoutSpec> spouts = new HashMap<>();
		spouts.put("A", spoutA);
		
		HashMap<String, Bolt> bolts = new HashMap<>();
		bolts.put("C", boltC);
		bolts.put("D", boltD);
		bolts.put("F", boltF);
		
		StormTopology topology = Mockito.mock(StormTopology.class);
		Mockito.when(topology.get_bolts()).thenReturn(bolts);
		Mockito.when(topology.get_spouts()).thenReturn(spouts);
		
		TopologyExplorer explorer = new TopologyExplorer("test", topology);
		
		ArrayList<String> childrenA = new ArrayList<>();
		childrenA.add("C");
		
		ArrayList<String> childrenC = new ArrayList<>();
		childrenC.add("D");
		childrenC.add("F");
		
		assertEquals(childrenA, explorer.getChildren("A"));
		assertEquals(childrenC, explorer.getChildren("C"));
		assertEquals(new ArrayList<>(), explorer.getChildren("D"));
		assertEquals(new ArrayList<>(), explorer.getChildren("F"));
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.explorer.TopologyExplorer#getAntecedents(java.lang.String)}.
	 */
	public final void testGetAntecedents() {
		GlobalStreamId gsA = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsA.get_componentId()).thenReturn("A");
		GlobalStreamId gsC = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsC.get_componentId()).thenReturn("C");
		GlobalStreamId gsD = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsD.get_componentId()).thenReturn("D");
		GlobalStreamId gsF = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsF.get_componentId()).thenReturn("F");
		Grouping grouping = Mockito.mock(Grouping.class);
		
		HashMap<GlobalStreamId, Grouping> inputsA = new HashMap<>();
		
		HashMap<GlobalStreamId, Grouping> inputsC = new HashMap<>();
		inputsC.put(gsA, grouping);
		
		HashMap<GlobalStreamId, Grouping> inputsD = new HashMap<>();
		inputsD.put(gsC, grouping);
		
		HashMap<GlobalStreamId, Grouping> inputsF = new HashMap<>();
		inputsF.put(gsC, grouping);
		
		ComponentCommon ccA = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccA.get_inputs()).thenReturn(inputsA);
		
		ComponentCommon ccC = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccC.get_inputs()).thenReturn(inputsC);
		
		ComponentCommon ccD = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccD.get_inputs()).thenReturn(inputsD);
		
		ComponentCommon ccF = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccF.get_inputs()).thenReturn(inputsF);
		
		SpoutSpec spoutA = Mockito.mock(SpoutSpec.class);
		Mockito.when(spoutA.get_common()).thenReturn(ccA);
		
		Bolt boltC = Mockito.mock(Bolt.class);
		Mockito.when(boltC.get_common()).thenReturn(ccC);
		
		Bolt boltD = Mockito.mock(Bolt.class);
		Mockito.when(boltD.get_common()).thenReturn(ccD);
		
		Bolt boltF = Mockito.mock(Bolt.class);
		Mockito.when(boltF.get_common()).thenReturn(ccF);
		
		HashMap<String, SpoutSpec> spouts = new HashMap<>();
		spouts.put("A", spoutA);
		
		HashMap<String, Bolt> bolts = new HashMap<>();
		bolts.put("C", boltC);
		bolts.put("D", boltD);
		bolts.put("F", boltF);
		
		StormTopology topology = Mockito.mock(StormTopology.class);
		Mockito.when(topology.get_bolts()).thenReturn(bolts);
		Mockito.when(topology.get_spouts()).thenReturn(spouts);
		
		TopologyExplorer explorer = new TopologyExplorer("test", topology);
		
		ArrayList<String> antecedentsDF = new ArrayList<>();
		antecedentsDF.add("C");
		
		assertEquals(new ArrayList<String>(), explorer.getAntecedents("A"));
		assertEquals(new ArrayList<String>(), explorer.getAntecedents("C"));
		assertEquals(antecedentsDF, explorer.getAntecedents("D"));
		assertEquals(antecedentsDF, explorer.getAntecedents("F"));
	}
}
