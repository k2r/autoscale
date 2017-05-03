/**
 * 
 */
package storm.autoscale.scheduler;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.modules.component.ComponentMonitor;
import storm.autoscale.scheduler.modules.explorer.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.StatStorageManager;

/**
 * @author Roland
 *
 */
public class ComponentMonitorTest {

	private XmlConfigParser parser;
	private TopologyExplorer explorer;
	private StatStorageManager manager;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		HashMap<Integer, Long> inputA = new HashMap<>();
		inputA.put(10, 380L);
		inputA.put(20, 470L);
		inputA.put(30, 550L);
		inputA.put(40, 600L);
		inputA.put(50, 650L);
		HashMap<Integer, Long> inputB = new HashMap<>();
		inputB.put(10, 350L);
		inputB.put(20, 450L);
		inputB.put(30, 520L);
		inputB.put(40, 680L);
		inputB.put(50, 620L);
		HashMap<Integer, Long> inputC = new HashMap<>();
		inputC.put(10, 350L);
		inputC.put(20, 450L);
		inputC.put(30, 520L);
		inputC.put(40, 680L);
		inputC.put(50, 620L);
		HashMap<Integer, Long> inputD = new HashMap<>();
		inputD.put(10, 250L);
		inputD.put(20, 320L);
		inputD.put(30, 360L);
		inputD.put(40, 400L);
		inputD.put(50, 430L);
		HashMap<Integer, Long> inputE = new HashMap<>();
		inputE.put(10, 300L);
		inputE.put(20, 390L);
		inputE.put(30, 450L);
		inputE.put(40, 500L);
		inputE.put(50, 530L);
		HashMap<Integer, Long> inputVoid = new HashMap<>();
		inputVoid.put(10, 0L);
		inputVoid.put(20, 0L);
		inputVoid.put(30, 0L);
		inputVoid.put(40, 0L);
		inputVoid.put(50, 0L);
		
		HashMap<Integer, Double> latencyA = new HashMap<>();
		latencyA.put(10, 10.0);
		latencyA.put(20, 10.0);
		latencyA.put(30, 10.0);
		latencyA.put(40, 10.0);
		latencyA.put(50, 10.0);
		HashMap<Integer, Double> latencyB = new HashMap<>();
		latencyB.put(10, 50.0);
		latencyB.put(20, 50.0);
		latencyB.put(30, 50.0);
		latencyB.put(40, 50.0);
		latencyB.put(50, 50.0);
		HashMap<Integer, Double> latencyC = new HashMap<>();
		latencyC.put(10, 20.0);
		latencyC.put(20, 20.0);
		latencyC.put(30, 20.0);
		latencyC.put(40, 20.0);
		latencyC.put(50, 20.0);
		HashMap<Integer, Double> latencyD = new HashMap<>();
		latencyD.put(10, 2.0);
		latencyD.put(20, 2.0);
		latencyD.put(30, 2.0);
		latencyD.put(40, 2.0);
		latencyD.put(50, 2.0);
		HashMap<Integer, Double> latencyE = new HashMap<>();
		latencyE.put(10, 50.0);
		latencyE.put(20, 50.0);
		latencyE.put(30, 50.0);
		latencyE.put(40, 50.0);
		latencyE.put(50, 50.0);
		
		HashMap<Integer, Double> latencyS = new HashMap<>();
		latencyS.put(10, 250.0);
		latencyS.put(20, 250.0);
		latencyS.put(30, 250.0);
		latencyS.put(40, 250.0);
		latencyS.put(50, 250.0);
		
		HashMap<Integer, Double> selectivityA = new HashMap<>();
		selectivityA.put(10, 1.0);
		selectivityA.put(20, 1.0);
		selectivityA.put(30, 1.0);
		selectivityA.put(40, 1.0);
		selectivityA.put(50, 1.0);
		HashMap<Integer, Double> selectivityB = new HashMap<>();
		selectivityB.put(10, 0.3);
		selectivityB.put(20, 0.3);
		selectivityB.put(30, 0.3);
		selectivityB.put(40, 0.3);
		selectivityB.put(50, 0.3);
		HashMap<Integer, Double> selectivityC = new HashMap<>();
		selectivityC.put(10, 0.7);
		selectivityC.put(20, 0.7);
		selectivityC.put(30, 0.7);
		selectivityC.put(40, 0.7);
		selectivityC.put(50, 0.7);
		HashMap<Integer, Double> selectivityD = new HashMap<>();
		selectivityD.put(10, 0.8);
		selectivityD.put(20, 0.8);
		selectivityD.put(30, 0.8);
		selectivityD.put(40, 0.8);
		selectivityD.put(50, 0.8);
		HashMap<Integer, Double> selectivityE = new HashMap<>();
		selectivityE.put(10, 1.0);
		selectivityE.put(20, 1.0);
		selectivityE.put(30, 1.0);
		selectivityE.put(40, 1.0);
		selectivityE.put(50, 1.0);
		
		HashSet<String> ancestors = new HashSet<>();
		ancestors.add("A");
		ArrayList<String> spouts = new ArrayList<>();
		spouts.add("S");
		ArrayList<String> bolts = new ArrayList<>();
		bolts.add("A");
		bolts.add("B");
		bolts.add("C");
		bolts.add("D");
		bolts.add("E");
		
		ArrayList<String> parentA = new ArrayList<>();
		parentA.add("S");
		ArrayList<String> parentB = new ArrayList<>();
		parentB.add("A");
		ArrayList<String> parentC = new ArrayList<>();
		parentC.add("A");
		ArrayList<String> parentD = new ArrayList<>();
		parentD.add("C");
		ArrayList<String> parentE = new ArrayList<>();
		parentE.add("B");
		
		ArrayList<String> childrenA = new ArrayList<>();
		childrenA.add("B");
		childrenA.add("C");
		ArrayList<String> childrenB = new ArrayList<>();
		childrenB.add("E");
		ArrayList<String> childrenC = new ArrayList<>();
		childrenC.add("D");
		ArrayList<String> childrenD = new ArrayList<>();
		childrenD.add("E");
		ArrayList<String> childrenE = new ArrayList<>();
		
		this.explorer = Mockito.mock(TopologyExplorer.class);
		Mockito.when(explorer.getTopologyName()).thenReturn("testTopology");
		Mockito.when(explorer.getSpouts()).thenReturn(spouts);
		Mockito.when(explorer.getBolts()).thenReturn(bolts);
		Mockito.when(explorer.getAncestors()).thenReturn(ancestors);
		Mockito.when(explorer.getParents("A")).thenReturn(parentA);
		Mockito.when(explorer.getParents("B")).thenReturn(parentB);
		Mockito.when(explorer.getParents("C")).thenReturn(parentC);
		Mockito.when(explorer.getParents("D")).thenReturn(parentD);
		Mockito.when(explorer.getParents("E")).thenReturn(parentE);
		
		Mockito.when(explorer.getChildren("A")).thenReturn(childrenA);
		Mockito.when(explorer.getChildren("B")).thenReturn(childrenB);
		Mockito.when(explorer.getChildren("C")).thenReturn(childrenC);
		Mockito.when(explorer.getChildren("D")).thenReturn(childrenD);
		Mockito.when(explorer.getChildren("E")).thenReturn(childrenE);
		
		this.parser = Mockito.mock(XmlConfigParser.class);
		Mockito.when(parser.getWindowSize()).thenReturn(50);
		Mockito.when(parser.getMonitoringFrequency()).thenReturn(10);
		Mockito.when(parser.getLowActivityThreshold()).thenReturn(0.3);
		
		Set<String> components = new HashSet<>();
		components.add("A");
		components.add("B");
		components.add("C");
		components.add("D");
		components.add("E");
		
		HashMap<String, Long> pending = new HashMap<>();
		pending.put("A", 100L);
		pending.put("B", 500L);
		pending.put("C", 1000L);
		pending.put("D", 0L);
		pending.put("E", 200L);
		
		HashMap<String, Double> cpuConstraints = new HashMap<>();
		cpuConstraints.put("A", 20.0);
		cpuConstraints.put("B", 25.0);
		cpuConstraints.put("C", 10.0);
		cpuConstraints.put("D", 30.0);
		cpuConstraints.put("E", 40.0);
		
		ArrayList<Double> cpuUsageA1 = new ArrayList<Double>();
		cpuUsageA1.add(15.0);
		cpuUsageA1.add(20.0);
		cpuUsageA1.add(15.0);
		ArrayList<Double> cpuUsageA2 = new ArrayList<Double>();
		cpuUsageA2.add(15.0);
		cpuUsageA2.add(20.0);
		cpuUsageA2.add(15.0);
		ArrayList<Double> cpuUsageA3 = new ArrayList<Double>();
		cpuUsageA3.add(15.0);
		cpuUsageA3.add(20.0);
		cpuUsageA3.add(15.0);
		ArrayList<Double> cpuUsageA4 = new ArrayList<Double>();
		cpuUsageA4.add(15.0);
		cpuUsageA4.add(20.0);
		cpuUsageA4.add(15.0);
		ArrayList<Double> cpuUsageA5 = new ArrayList<Double>();
		cpuUsageA5.add(15.0);
		cpuUsageA5.add(20.0);
		cpuUsageA5.add(15.0);
		
		HashMap<Integer, ArrayList<Double>> cpuUsageA = new HashMap<>();
		cpuUsageA.put(10, cpuUsageA1);
		cpuUsageA.put(20, cpuUsageA2);
		cpuUsageA.put(30, cpuUsageA3);
		cpuUsageA.put(40, cpuUsageA4);
		cpuUsageA.put(50, cpuUsageA5);
		
		
		ArrayList<Double> cpuUsageB1 = new ArrayList<Double>();
		cpuUsageB1.add(20.0);
		cpuUsageB1.add(18.0);
		cpuUsageB1.add(30.0);
		cpuUsageB1.add(30.0);
		ArrayList<Double> cpuUsageB2 = new ArrayList<Double>();
		cpuUsageB2.add(20.0);
		cpuUsageB2.add(18.0);
		cpuUsageB2.add(30.0);
		cpuUsageB2.add(30.0);
		ArrayList<Double> cpuUsageB3 = new ArrayList<Double>();
		cpuUsageB3.add(20.0);
		cpuUsageB3.add(18.0);
		cpuUsageB3.add(30.0);
		cpuUsageB3.add(30.0);
		ArrayList<Double> cpuUsageB4 = new ArrayList<Double>();
		cpuUsageB4.add(20.0);
		cpuUsageB4.add(18.0);
		cpuUsageB4.add(30.0);
		cpuUsageB4.add(30.0);
		ArrayList<Double> cpuUsageB5 = new ArrayList<Double>();
		cpuUsageB5.add(20.0);
		cpuUsageB5.add(18.0);
		cpuUsageB5.add(30.0);
		cpuUsageB5.add(30.0);
		
		HashMap<Integer, ArrayList<Double>> cpuUsageB = new HashMap<>();
		cpuUsageB.put(10, cpuUsageB1);
		cpuUsageB.put(20, cpuUsageB2);
		cpuUsageB.put(30, cpuUsageB3);
		cpuUsageB.put(40, cpuUsageB4);
		cpuUsageB.put(50, cpuUsageB5);
		
		ArrayList<Double> cpuUsageC1 = new ArrayList<Double>();
		cpuUsageC1.add(15.0);
		cpuUsageC1.add(20.0);
		ArrayList<Double> cpuUsageC2 = new ArrayList<Double>();
		cpuUsageC2.add(15.0);
		cpuUsageC2.add(20.0);
		ArrayList<Double> cpuUsageC3 = new ArrayList<Double>();
		cpuUsageC3.add(15.0);
		cpuUsageC3.add(20.0);
		ArrayList<Double> cpuUsageC4 = new ArrayList<Double>();
		cpuUsageC4.add(15.0);
		cpuUsageC4.add(20.0);
		ArrayList<Double> cpuUsageC5 = new ArrayList<Double>();
		cpuUsageC5.add(15.0);
		cpuUsageC5.add(20.0);
		
		HashMap<Integer, ArrayList<Double>> cpuUsageC = new HashMap<>();
		cpuUsageC.put(10, cpuUsageC1);
		cpuUsageC.put(20, cpuUsageC2);
		cpuUsageC.put(30, cpuUsageC3);
		cpuUsageC.put(40, cpuUsageC4);
		cpuUsageC.put(50, cpuUsageC5);
		
		ArrayList<Double> cpuUsageD1 = new ArrayList<Double>();
		cpuUsageD1.add(25.0);
		cpuUsageD1.add(40.0);
		ArrayList<Double> cpuUsageD2 = new ArrayList<Double>();
		cpuUsageD2.add(25.0);
		cpuUsageD2.add(40.0);
		ArrayList<Double> cpuUsageD3 = new ArrayList<Double>();
		cpuUsageD3.add(25.0);
		cpuUsageD3.add(40.0);
		ArrayList<Double> cpuUsageD4 = new ArrayList<Double>();
		cpuUsageD4.add(25.0);
		cpuUsageD4.add(40.0);
		ArrayList<Double> cpuUsageD5 = new ArrayList<Double>();
		cpuUsageD5.add(25.0);
		cpuUsageD5.add(40.0);
		
		HashMap<Integer, ArrayList<Double>> cpuUsageD = new HashMap<>();
		cpuUsageD.put(10, cpuUsageD1);
		cpuUsageD.put(20, cpuUsageD2);
		cpuUsageD.put(30, cpuUsageD3);
		cpuUsageD.put(40, cpuUsageD4);
		cpuUsageD.put(50, cpuUsageD5);
		
		ArrayList<Double> cpuUsageE1 = new ArrayList<Double>();
		cpuUsageE1.add(40.0);
		cpuUsageE1.add(60.0);
		cpuUsageE1.add(80.0);
		ArrayList<Double> cpuUsageE2 = new ArrayList<Double>();
		cpuUsageE2.add(40.0);
		cpuUsageE2.add(60.0);
		cpuUsageE2.add(80.0);
		ArrayList<Double> cpuUsageE3 = new ArrayList<Double>();
		cpuUsageE3.add(40.0);
		cpuUsageE3.add(60.0);
		cpuUsageE3.add(80.0);
		ArrayList<Double> cpuUsageE4 = new ArrayList<Double>();
		cpuUsageE4.add(40.0);
		cpuUsageE4.add(60.0);
		cpuUsageE4.add(80.0);
		ArrayList<Double> cpuUsageE5 = new ArrayList<Double>();
		cpuUsageE5.add(40.0);
		cpuUsageE5.add(60.0);
		cpuUsageE5.add(80.0);
		
		HashMap<Integer, ArrayList<Double>> cpuUsageE = new HashMap<>();
		cpuUsageE.put(10, cpuUsageE1);
		cpuUsageE.put(20, cpuUsageE2);
		cpuUsageE.put(30, cpuUsageE3);
		cpuUsageE.put(40, cpuUsageE4);
		cpuUsageE.put(50, cpuUsageE5);
	
		
		this.manager = Mockito.mock(StatStorageManager.class);
		
		Mockito.when(manager.getCurrentTimestamp()).thenReturn(50);
		Mockito.when(manager.getSpoutOutputs("S", 50, 50)).thenReturn(inputA);
		Mockito.when(manager.getBoltOutputs("A", 50, 50)).thenReturn(inputB);
		Mockito.when(manager.getBoltOutputs("B", 50, 50)).thenReturn(inputE);
		Mockito.when(manager.getBoltOutputs("C", 50, 50)).thenReturn(inputD);
		Mockito.when(manager.getBoltOutputs("D", 50, 50)).thenReturn(inputVoid);
		Mockito.when(manager.getBoltOutputs("E", 50, 50)).thenReturn(inputVoid);
		Mockito.when(manager.getExecuted("A", 50, 50)).thenReturn(inputB);
		Mockito.when(manager.getExecuted("B", 50, 50)).thenReturn(inputE);
		Mockito.when(manager.getExecuted("C", 50, 50)).thenReturn(inputD);
		Mockito.when(manager.getExecuted("D", 50, 50)).thenReturn(inputVoid);
		Mockito.when(manager.getExecuted("E", 50, 50)).thenReturn(inputVoid);
		Mockito.when(manager.getTopologyAvgLatency("testTopology", 50, 50)).thenReturn(latencyS);
		Mockito.when(manager.getAvgLatency("A", 50, 50)).thenReturn(latencyA);
		Mockito.when(manager.getAvgLatency("B", 50, 50)).thenReturn(latencyB);
		Mockito.when(manager.getAvgLatency("C", 50, 50)).thenReturn(latencyC);
		Mockito.when(manager.getAvgLatency("D", 50, 50)).thenReturn(latencyD);
		Mockito.when(manager.getAvgLatency("E", 50, 50)).thenReturn(latencyE);
		Mockito.when(manager.getSelectivity("A", 50, 50)).thenReturn(selectivityA);
		Mockito.when(manager.getSelectivity("B", 50, 50)).thenReturn(selectivityB);
		Mockito.when(manager.getSelectivity("C", 50, 50)).thenReturn(selectivityC);
		Mockito.when(manager.getSelectivity("D", 50, 50)).thenReturn(selectivityD);
		Mockito.when(manager.getSelectivity("E", 50, 50)).thenReturn(selectivityE);
		Mockito.when(manager.getCpuUsage("A", 50, 50)).thenReturn(cpuUsageA);
		Mockito.when(manager.getCpuUsage("B", 50, 50)).thenReturn(cpuUsageB);
		Mockito.when(manager.getCpuUsage("C", 50, 50)).thenReturn(cpuUsageC);
		Mockito.when(manager.getCpuUsage("D", 50, 50)).thenReturn(cpuUsageD);
		Mockito.when(manager.getCpuUsage("E", 50, 50)).thenReturn(cpuUsageE);
		Mockito.when(manager.getInitialCpuConstraint("testTopology", "A")).thenReturn(20.0);
		Mockito.when(manager.getInitialCpuConstraint("testTopology", "B")).thenReturn(25.0);
		Mockito.when(manager.getInitialCpuConstraint("testTopology", "C")).thenReturn(10.0);
		Mockito.when(manager.getInitialCpuConstraint("testTopology", "D")).thenReturn(30.0);
		Mockito.when(manager.getInitialCpuConstraint("testTopology", "E")).thenReturn(40.0);
		Mockito.when(manager.getCurrentUpdateOutput(50, 50, "S", StatStorageManager.TABLE_SPOUT)).thenReturn(1000L);
		Mockito.when(manager.getCurrentUpdateOutput(50, 50, "A", StatStorageManager.TABLE_BOLT)).thenReturn(1000L);
		Mockito.when(manager.getCurrentUpdateOutput(50, 50, "B", StatStorageManager.TABLE_BOLT)).thenReturn(1000L);
		Mockito.when(manager.getCurrentUpdateOutput(50, 50, "C", StatStorageManager.TABLE_BOLT)).thenReturn(1000L);
		Mockito.when(manager.getCurrentUpdateOutput(50, 50, "D", StatStorageManager.TABLE_BOLT)).thenReturn(1000L);
		Mockito.when(manager.getCurrentUpdateExecuted(50, 50, "A", StatStorageManager.TABLE_BOLT)).thenReturn(900L);
		Mockito.when(manager.getCurrentUpdateExecuted(50, 50, "B", StatStorageManager.TABLE_BOLT)).thenReturn(500L);
		Mockito.when(manager.getCurrentUpdateExecuted(50, 50, "C", StatStorageManager.TABLE_BOLT)).thenReturn(0L);
		Mockito.when(manager.getCurrentUpdateExecuted(50, 50, "D", StatStorageManager.TABLE_BOLT)).thenReturn(1000L);
		Mockito.when(manager.getCurrentUpdateExecuted(50, 50, "E", StatStorageManager.TABLE_BOLT)).thenReturn(800L);
		
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.component.ComponentMonitor#getStatistics(storm.autoscale.scheduler.modules.explorer.TopologyExplorer)}.
	 */
	@Test
	public void testGetStatistics() {
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.setManager(manager);
		cm.getStatistics(explorer);
		HashMap<Integer, Long> inputA = new HashMap<>();
		inputA.put(10, 380L);
		inputA.put(20, 470L);
		inputA.put(30, 550L);
		inputA.put(40, 600L);
		inputA.put(50, 650L);
		HashMap<Integer, Long> inputB = new HashMap<>();
		inputB.put(10, 350L);
		inputB.put(20, 450L);
		inputB.put(30, 520L);
		inputB.put(40, 680L);
		inputB.put(50, 620L);
		HashMap<Integer, Long> inputC = new HashMap<>();
		inputC.put(10, 350L);
		inputC.put(20, 450L);
		inputC.put(30, 520L);
		inputC.put(40, 680L);
		inputC.put(50, 620L);
		HashMap<Integer, Long> inputD = new HashMap<>();
		inputD.put(10, 250L);
		inputD.put(20, 320L);
		inputD.put(30, 360L);
		inputD.put(40, 400L);
		inputD.put(50, 430L);
		HashMap<Integer, Long> inputE = new HashMap<>();
		inputE.put(10, 300L);
		inputE.put(20, 390L);
		inputE.put(30, 450L);
		inputE.put(40, 500L);
		inputE.put(50, 530L);
		
		assertEquals(inputA, cm.getStats("A").getInputRecords());
		assertEquals(inputB, cm.getStats("B").getInputRecords());
		assertEquals(inputC, cm.getStats("C").getInputRecords());
		assertEquals(inputD, cm.getStats("D").getInputRecords());
		assertEquals(inputE, cm.getStats("E").getInputRecords());
		
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.component.ComponentMonitor#getRegisteredComponents()}.
	 */
	@Test
	public void testGetRegisteredComponents() {
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.setManager(manager);
		cm.getStatistics(explorer);
		Set<String> components = new HashSet<>();
		components.add("S");
		components.add("A");
		components.add("B");
		components.add("C");
		components.add("D");
		components.add("E");
		
		assertEquals(components, cm.getRegisteredComponents());
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.component.ComponentMonitor#getTimestamp()}.
	 */
	@Test
	public void testGetTimestamp() {
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.setManager(manager);
		cm.getStatistics(explorer);
		assertEquals(50, cm.getTimestamp(), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.component.ComponentMonitor#getMonitoringFrequency()}.
	 */
	@Test
	public void testGetMonitoringFrequency() {
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.setManager(manager);
		cm.getStatistics(explorer);
		assertEquals(10, cm.getMonitoringFrequency(), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.component.ComponentMonitor#getParser()}.
	 */
	@Test
	public void testGetParser() {
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.setManager(manager);
		cm.getStatistics(explorer);
		assertEquals(this.parser, cm.getParser());
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.component.ComponentMonitor#getPendingTuples(storm.autoscale.scheduler.modules.explorer.TopologyExplorer)}.
	 */
	@Test
	public void testGetPendingTuples() {
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.setManager(manager);
		cm.getStatistics(explorer);
		HashMap<String, Long> pending = new HashMap<>();
		pending.put("S", 0L);
		pending.put("A", 100L);
		pending.put("B", 500L);
		pending.put("C", 1000L);
		pending.put("D", 0L);
		pending.put("E", 200L);
		
		assertEquals(pending, cm.getPendingTuples(explorer));
	}
}