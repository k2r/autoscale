/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import org.mockito.Mockito;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import junit.framework.TestCase;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

/**
 * @author Roland
 *
 */
public class ComponentMonitorTest extends TestCase {

	Long scaleFactor = 5L;
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#isInputDecreasing(java.lang.String)}.
	 */
	public void testIsInputDecreasing() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 250L / this.scaleFactor);
		inputRecords1.put(9, 260L / this.scaleFactor);
		inputRecords1.put(8, 425L / this.scaleFactor);
		inputRecords1.put(7, 530L / this.scaleFactor);
		inputRecords1.put(6, 710L / this.scaleFactor);
		inputRecords1.put(5, 780L / this.scaleFactor);
		inputRecords1.put(4, 955L / this.scaleFactor);
		inputRecords1.put(3, 1095L / this.scaleFactor);
		inputRecords1.put(2, 1225L / this.scaleFactor);
		inputRecords1.put(1, 1350L / this.scaleFactor);
	
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 250L / this.scaleFactor);
		inputRecords2.put(9, 260L / this.scaleFactor);
		inputRecords2.put(8, 425L / this.scaleFactor);
		inputRecords2.put(7, 455L / this.scaleFactor);
		inputRecords2.put(6, 710L / this.scaleFactor);
		inputRecords2.put(5, 780L / this.scaleFactor);
		inputRecords2.put(4, 800L / this.scaleFactor);
		inputRecords2.put(3, 1095L / this.scaleFactor);
		inputRecords2.put(2, 1225L / this.scaleFactor);
		inputRecords2.put(1, 1350L / this.scaleFactor);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, null, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords2, null, null, null, null);
		
		ComponentMonitor cm = new ComponentMonitor(null, null, null, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		
		assertEquals(false, cm.isInputDecreasing("component1"));
		assertEquals(false, cm.isInputDecreasing("component2"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#isInputStable(java.lang.String)}.
	 */
	public void testIsInputStable() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 1350L / this.scaleFactor);
		inputRecords1.put(9, 1225L / this.scaleFactor);
		inputRecords1.put(8, 1095L / this.scaleFactor);
		inputRecords1.put(7, 955L / this.scaleFactor);
		inputRecords1.put(6, 780L / this.scaleFactor);
		inputRecords1.put(5, 710L / this.scaleFactor);
		inputRecords1.put(4, 530L / this.scaleFactor);
		inputRecords1.put(3, 425L / this.scaleFactor);
		inputRecords1.put(2, 260L / this.scaleFactor);
		inputRecords1.put(1, 250L / this.scaleFactor);
	
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 220L / this.scaleFactor);
		inputRecords2.put(9, 215L / this.scaleFactor);
		inputRecords2.put(8, 209L / this.scaleFactor);
		inputRecords2.put(7, 198L / this.scaleFactor);
		inputRecords2.put(6, 116L / this.scaleFactor);
		inputRecords2.put(5, 102L / this.scaleFactor);
		inputRecords2.put(4, 455L / this.scaleFactor);
		inputRecords2.put(3, 425L / this.scaleFactor);
		inputRecords2.put(2, 260L / this.scaleFactor);
		inputRecords2.put(1, 250L / this.scaleFactor);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, null, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords2, null, null, null, null);
		
		ComponentMonitor cm = new ComponentMonitor(null, null, null, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		
		assertEquals(true, cm.isInputStable("component1"));
		assertEquals(true, cm.isInputStable("component2"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#isInputIncreasing(java.lang.String)}.
	 */
	public void testIsInputIncreasing() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 1350L / this.scaleFactor);
		inputRecords1.put(9, 1225L / this.scaleFactor);
		inputRecords1.put(8, 1095L / this.scaleFactor);
		inputRecords1.put(7, 955L / this.scaleFactor);
		inputRecords1.put(6, 780L / this.scaleFactor);
		inputRecords1.put(5, 710L / this.scaleFactor);
		inputRecords1.put(4, 530L / this.scaleFactor);
		inputRecords1.put(3, 425L / this.scaleFactor);
		inputRecords1.put(2, 260L / this.scaleFactor);
		inputRecords1.put(1, 250L / this.scaleFactor);
	
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 260L / this.scaleFactor);
		inputRecords2.put(9, 235L / this.scaleFactor);
		inputRecords2.put(8, 1095L / this.scaleFactor);
		inputRecords2.put(7, 160L / this.scaleFactor);
		inputRecords2.put(6, 780L / this.scaleFactor);
		inputRecords2.put(5, 710L / this.scaleFactor);
		inputRecords2.put(4, 455L / this.scaleFactor);
		inputRecords2.put(3, 425L / this.scaleFactor);
		inputRecords2.put(2, 260L / this.scaleFactor);
		inputRecords2.put(1, 250L / this.scaleFactor);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, null, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords2, null, null, null, null);
		
		ComponentMonitor cm = new ComponentMonitor(null, null, null, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		
		assertEquals(false, cm.isInputIncreasing("component1"));
		assertEquals(false, cm.isInputIncreasing("component2"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#needScaleOut(java.lang.String)}.
	 */
	public void testIsCongested() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 1350L / this.scaleFactor);
		inputRecords1.put(9, 1225L / this.scaleFactor);
		inputRecords1.put(8, 1095L / this.scaleFactor);
		inputRecords1.put(7, 955L / this.scaleFactor);
		inputRecords1.put(6, 780L / this.scaleFactor);
		inputRecords1.put(5, 710L / this.scaleFactor);
		inputRecords1.put(4, 530L / this.scaleFactor);
		inputRecords1.put(3, 425L / this.scaleFactor);
		inputRecords1.put(2, 260L / this.scaleFactor);
		inputRecords1.put(1, 250L / this.scaleFactor);
		
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 250L / this.scaleFactor);
		inputRecords2.put(9, 260L / this.scaleFactor);
		inputRecords2.put(8, 425L / this.scaleFactor);
		inputRecords2.put(7, 530L / this.scaleFactor);
		inputRecords2.put(6, 710L / this.scaleFactor);
		inputRecords2.put(5, 780L / this.scaleFactor);
		inputRecords2.put(4, 955L / this.scaleFactor);
		inputRecords2.put(3, 1095L / this.scaleFactor);
		inputRecords2.put(2, 1225L / this.scaleFactor);
		inputRecords2.put(1, 1350L / this.scaleFactor);
		
		HashMap<Integer, Long> executedRecords1 = new HashMap<>();
		executedRecords1.put(10, 295L / this.scaleFactor);
		executedRecords1.put(9, 305L / this.scaleFactor);
		executedRecords1.put(8, 320L / this.scaleFactor);
		executedRecords1.put(7, 295L / this.scaleFactor);
		executedRecords1.put(6, 310L / this.scaleFactor);
		executedRecords1.put(5, 300L / this.scaleFactor);
		executedRecords1.put(4, 305L / this.scaleFactor);
		executedRecords1.put(3, 290L / this.scaleFactor);
		executedRecords1.put(2, 280L / this.scaleFactor);
		executedRecords1.put(1, 275L / this.scaleFactor);
		
		HashMap<Integer, Long> executedRecords2 = new HashMap<>();
		executedRecords2.put(10, 1150L / this.scaleFactor);
		executedRecords2.put(9, 1135L / this.scaleFactor);
		executedRecords2.put(8, 1160L / this.scaleFactor);
		executedRecords2.put(7, 825L / this.scaleFactor);
		executedRecords2.put(6, 795L / this.scaleFactor);
		executedRecords2.put(5, 715L / this.scaleFactor);
		executedRecords2.put(4, 590L / this.scaleFactor);
		executedRecords2.put(3, 290L / this.scaleFactor);
		executedRecords2.put(2, 280L / this.scaleFactor);
		executedRecords2.put(1, 275L / this.scaleFactor);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, executedRecords1, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords1, executedRecords2, null, null, null);
		ComponentWindowedStats cws3 = new ComponentWindowedStats("component3", inputRecords2, executedRecords1, null, null, null);
		ComponentWindowedStats cws4 = new ComponentWindowedStats("component4", inputRecords2, executedRecords2, null, null, null);
		
		ComponentMonitor cm = new ComponentMonitor(null, null, null, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		cm.updateStats(cws3.getId(), cws3);
		cm.updateStats(cws4.getId(), cws4);
		
		assertEquals(false, cm.needScaleOut("component1"));
		assertEquals(false, cm.needScaleOut("component2"));
		assertEquals(false, cm.needScaleOut("component3"));
		assertEquals(false, cm.needScaleOut("component4"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.stats.ComponentMonitor#getScaleOutDecisions()}.
	 */
	public void testGetCongested() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 1350L / this.scaleFactor);
		inputRecords1.put(9, 1225L / this.scaleFactor);
		inputRecords1.put(8, 1095L / this.scaleFactor);
		inputRecords1.put(7, 955L / this.scaleFactor);
		inputRecords1.put(6, 780L / this.scaleFactor);
		inputRecords1.put(5, 710L / this.scaleFactor);
		inputRecords1.put(4, 530L / this.scaleFactor);
		inputRecords1.put(3, 425L / this.scaleFactor);
		inputRecords1.put(2, 260L / this.scaleFactor);
		inputRecords1.put(1, 250L / this.scaleFactor);
		
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 250L / this.scaleFactor);
		inputRecords2.put(9, 260L / this.scaleFactor);
		inputRecords2.put(8, 425L / this.scaleFactor);
		inputRecords2.put(7, 530L / this.scaleFactor);
		inputRecords2.put(6, 710L / this.scaleFactor);
		inputRecords2.put(5, 780L / this.scaleFactor);
		inputRecords2.put(4, 955L / this.scaleFactor);
		inputRecords2.put(3, 1095L / this.scaleFactor);
		inputRecords2.put(2, 1225L / this.scaleFactor);
		inputRecords2.put(1, 1350L / this.scaleFactor);
		
		HashMap<Integer, Long> executedRecords1 = new HashMap<>();
		executedRecords1.put(10, 295L / this.scaleFactor);
		executedRecords1.put(9, 305L / this.scaleFactor);
		executedRecords1.put(8, 320L / this.scaleFactor);
		executedRecords1.put(7, 295L / this.scaleFactor);
		executedRecords1.put(6, 310L / this.scaleFactor);
		executedRecords1.put(5, 300L / this.scaleFactor);
		executedRecords1.put(4, 305L / this.scaleFactor);
		executedRecords1.put(3, 290L / this.scaleFactor);
		executedRecords1.put(2, 280L / this.scaleFactor);
		executedRecords1.put(1, 275L / this.scaleFactor);
		
		HashMap<Integer, Long> executedRecords2 = new HashMap<>();
		executedRecords2.put(10, 1150L / this.scaleFactor);
		executedRecords2.put(9, 1135L / this.scaleFactor);
		executedRecords2.put(8, 1160L / this.scaleFactor);
		executedRecords2.put(7, 825L / this.scaleFactor);
		executedRecords2.put(6, 795L / this.scaleFactor);
		executedRecords2.put(5, 715L / this.scaleFactor);
		executedRecords2.put(4, 590L / this.scaleFactor);
		executedRecords2.put(3, 290L / this.scaleFactor);
		executedRecords2.put(2, 280L / this.scaleFactor);
		executedRecords2.put(1, 275L / this.scaleFactor);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, executedRecords1, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords1, executedRecords2, null, null, null);
		ComponentWindowedStats cws3 = new ComponentWindowedStats("component3", inputRecords2, executedRecords1, null, null, null);
		ComponentWindowedStats cws4 = new ComponentWindowedStats("component4", inputRecords2, executedRecords2, null, null, null);
		
		ComponentMonitor cm = new ComponentMonitor(null, null, null, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		cm.updateStats(cws3.getId(), cws3);
		cm.updateStats(cws4.getId(), cws4);
		
		ArrayList<String> expected = new ArrayList<>();
		assertEquals(expected, cm.getScaleOutRequirements());
	}

	public void testValidateRequirements(){
		GlobalStreamId gsA = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsA.get_componentId()).thenReturn("A");
		GlobalStreamId gsB = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsB.get_componentId()).thenReturn("B");
		GlobalStreamId gsC = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsC.get_componentId()).thenReturn("C");
		GlobalStreamId gsD = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsD.get_componentId()).thenReturn("D");
		GlobalStreamId gsE = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsE.get_componentId()).thenReturn("E");
		GlobalStreamId gsF = Mockito.mock(GlobalStreamId.class);
		Mockito.when(gsF.get_componentId()).thenReturn("F");
		Grouping grouping = Mockito.mock(Grouping.class);
		
		HashMap<GlobalStreamId, Grouping> inputsA = new HashMap<>();
		
		HashMap<GlobalStreamId, Grouping> inputsB = new HashMap<>();
		inputsB.put(gsA, grouping);
		
		HashMap<GlobalStreamId, Grouping> inputsC = new HashMap<>();
		inputsC.put(gsB, grouping);
		
		HashMap<GlobalStreamId, Grouping> inputsD = new HashMap<>();
		inputsD.put(gsC, grouping);
		
		HashMap<GlobalStreamId, Grouping> inputsE = new HashMap<>();
		inputsE.put(gsC, grouping);
		
		HashMap<GlobalStreamId, Grouping> inputsF = new HashMap<>();
		inputsF.put(gsE, grouping);
		
		ComponentCommon ccA = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccA.get_inputs()).thenReturn(inputsA);
		
		ComponentCommon ccB = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccB.get_inputs()).thenReturn(inputsB);
		
		ComponentCommon ccC = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccC.get_inputs()).thenReturn(inputsC);
		
		ComponentCommon ccD = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccD.get_inputs()).thenReturn(inputsD);
		
		ComponentCommon ccE = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccE.get_inputs()).thenReturn(inputsE);
		
		ComponentCommon ccF = Mockito.mock(ComponentCommon.class);
		Mockito.when(ccF.get_inputs()).thenReturn(inputsF);
		
		SpoutSpec spoutA = Mockito.mock(SpoutSpec.class);
		Mockito.when(spoutA.get_common()).thenReturn(ccA);
		
		Bolt boltB = Mockito.mock(Bolt.class);
		Mockito.when(boltB.get_common()).thenReturn(ccB);
		
		Bolt boltC = Mockito.mock(Bolt.class);
		Mockito.when(boltC.get_common()).thenReturn(ccC);
		
		Bolt boltD = Mockito.mock(Bolt.class);
		Mockito.when(boltD.get_common()).thenReturn(ccD);
		
		Bolt boltE = Mockito.mock(Bolt.class);
		Mockito.when(boltE.get_common()).thenReturn(ccE);
		
		Bolt boltF = Mockito.mock(Bolt.class);
		Mockito.when(boltF.get_common()).thenReturn(ccF);
		
		HashMap<String, SpoutSpec> spouts = new HashMap<>();
		spouts.put("A", spoutA);
		
		HashMap<String, Bolt> bolts = new HashMap<>();
		bolts.put("B", boltB);
		bolts.put("C", boltC);
		bolts.put("D", boltD);
		bolts.put("E", boltE);
		bolts.put("F", boltF);
		
		StormTopology topology = Mockito.mock(StormTopology.class);
		Mockito.when(topology.get_bolts()).thenReturn(bolts);
		Mockito.when(topology.get_spouts()).thenReturn(spouts);
		
		TopologyExplorer explorer = new TopologyExplorer("test", topology);
		
		ComponentMonitor compMonitor = new ComponentMonitor(null, null, null, null, null);
		
		HashMap<String, Double> eprValues = new HashMap<>();
		eprValues.put("A", -1.0);
		eprValues.put("B", 0.5);
		eprValues.put("C", 2.0);
		eprValues.put("D", -1.0);
		eprValues.put("E", 0.5);
		eprValues.put("F", 2.0);
		
		ArrayList<String> scaleInRequirements = new ArrayList<>();
		scaleInRequirements.add("B");
		scaleInRequirements.add("E");
		
		ArrayList<String> scaleOutRequirements = new ArrayList<>();
		scaleOutRequirements.add("C");
		scaleOutRequirements.add("F");
		
		compMonitor.setScaleInRequirements(scaleInRequirements);
		compMonitor.setScaleOutRequirements(scaleOutRequirements);
		compMonitor.setEprValues(eprValues);
		
		compMonitor.validateRequirements(explorer.getSpouts(), explorer);
		
		ArrayList<String> expectedScaleIn = new ArrayList<>();
		expectedScaleIn.add("B");
		
		ArrayList<String> expectedScaleOut = new ArrayList<>();
		expectedScaleOut.add("C");
		expectedScaleOut.add("F");
		
		assertEquals(expectedScaleIn, compMonitor.getScaleInRequirements());
		assertEquals(expectedScaleOut, compMonitor.getScaleOutRequirements());
	}
}
