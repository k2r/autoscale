/**
 * 
 */
package storm.autoscale.scheduler;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.storm.scheduler.WorkerSlot;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.modules.assignment.AssignmentMonitor;
import storm.autoscale.scheduler.modules.component.ComponentMonitor;
import storm.autoscale.scheduler.modules.explorer.TopologyExplorer;
import storm.autoscale.scheduler.modules.scale.ScalingManager3;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.modules.stats.StatStorageManager;

/**
 * @author Roland
 *
 */
public class ScalingManager3Test {

	private ComponentMonitor cm;
	private TopologyExplorer explorer;
	private AssignmentMonitor am;
	
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
		
		ComponentWindowedStats statsA = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(statsA.getInputRecords()).thenReturn(inputA);
		Mockito.when(statsA.getAvgLatencyRecords()).thenReturn(latencyA);
		Mockito.when(statsA.getSelectivityRecords()).thenReturn(selectivityA);
		
		ComponentWindowedStats statsB = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(statsB.getInputRecords()).thenReturn(inputB);
		Mockito.when(statsB.getAvgLatencyRecords()).thenReturn(latencyB);
		Mockito.when(statsB.getSelectivityRecords()).thenReturn(selectivityB);
		
		ComponentWindowedStats statsC = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(statsC.getInputRecords()).thenReturn(inputC);
		Mockito.when(statsC.getAvgLatencyRecords()).thenReturn(latencyC);
		Mockito.when(statsC.getSelectivityRecords()).thenReturn(selectivityC);
		
		ComponentWindowedStats statsD = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(statsD.getInputRecords()).thenReturn(inputD);
		Mockito.when(statsD.getAvgLatencyRecords()).thenReturn(latencyD);
		Mockito.when(statsD.getSelectivityRecords()).thenReturn(selectivityD);
		
		ComponentWindowedStats statsE = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(statsE.getInputRecords()).thenReturn(inputE);
		Mockito.when(statsE.getAvgLatencyRecords()).thenReturn(latencyE);
		Mockito.when(statsE.getSelectivityRecords()).thenReturn(selectivityE);
		
		HashSet<String> ancestors = new HashSet<>();
		ancestors.add("A");
		ArrayList<String> bolts = new ArrayList<>();
		bolts.add("A");
		bolts.add("B");
		bolts.add("C");
		bolts.add("D");
		bolts.add("E");
		
		ArrayList<String> parentA = new ArrayList<>();
		ArrayList<String> parentB = new ArrayList<>();
		parentB.add("A");
		ArrayList<String> parentC = new ArrayList<>();
		parentC.add("A");
		ArrayList<String> parentD = new ArrayList<>();
		parentD.add("C");
		ArrayList<String> parentE = new ArrayList<>();
		parentE.add("B");
		parentE.add("D");
		
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
		
		WorkerSlot slot1 = Mockito.mock(WorkerSlot.class);
		Mockito.when(slot1.getNodeId()).thenReturn("supervisor1");
		Mockito.when(slot1.getPort()).thenReturn(6700);
		WorkerSlot slot2 = Mockito.mock(WorkerSlot.class);
		Mockito.when(slot2.getNodeId()).thenReturn("supervisor1");
		Mockito.when(slot2.getPort()).thenReturn(6701);
		WorkerSlot slot3 = Mockito.mock(WorkerSlot.class);
		Mockito.when(slot3.getNodeId()).thenReturn("supervisor1");
		Mockito.when(slot3.getPort()).thenReturn(6702);
		WorkerSlot slot4 = Mockito.mock(WorkerSlot.class);
		Mockito.when(slot4.getNodeId()).thenReturn("supervisor1");
		Mockito.when(slot4.getPort()).thenReturn(6703);
		WorkerSlot slot5 = Mockito.mock(WorkerSlot.class);
		Mockito.when(slot5.getNodeId()).thenReturn("supervisor2");
		Mockito.when(slot5.getPort()).thenReturn(6700);
		WorkerSlot slot6 = Mockito.mock(WorkerSlot.class);
		Mockito.when(slot6.getNodeId()).thenReturn("supervisor2");
		Mockito.when(slot6.getPort()).thenReturn(6701);
		WorkerSlot slot7 = Mockito.mock(WorkerSlot.class);
		Mockito.when(slot7.getNodeId()).thenReturn("supervisor2");
		Mockito.when(slot7.getPort()).thenReturn(6702);
		WorkerSlot slot8 = Mockito.mock(WorkerSlot.class);
		Mockito.when(slot8.getNodeId()).thenReturn("supervisor2");
		Mockito.when(slot8.getPort()).thenReturn(6703);
		
		ArrayList<String> assignment1 = new ArrayList<>();
		assignment1.add("A");
		assignment1.add("B");
		ArrayList<String> assignment2 = new ArrayList<>();
		assignment2.add("A");
		assignment2.add("B");
		ArrayList<String> assignment3 = new ArrayList<>();
		assignment3.add("B");
		assignment3.add("C");
		assignment3.add("D");
		ArrayList<String> assignment4 = new ArrayList<>();
		assignment4.add("B");
		ArrayList<String> assignment5 = new ArrayList<>();
		assignment5.add("A");
		ArrayList<String> assignment6 = new ArrayList<>();
		assignment6.add("D");
		assignment6.add("E");
		ArrayList<String> assignment7 = new ArrayList<>();
		assignment7.add("C");
		assignment7.add("E");
		ArrayList<String> assignment8 = new ArrayList<>();
		assignment8.add("E");
		
		ArrayList<WorkerSlot> workerA = new ArrayList<>();
		workerA.add(slot1);
		workerA.add(slot2);
		workerA.add(slot5);
		ArrayList<WorkerSlot> workerB = new ArrayList<>();
		workerB.add(slot1);
		workerB.add(slot2);
		workerB.add(slot3);
		workerB.add(slot4);
		ArrayList<WorkerSlot> workerC = new ArrayList<>();
		workerC.add(slot3);
		workerC.add(slot7);
		ArrayList<WorkerSlot> workerD = new ArrayList<>();
		workerD.add(slot3);
		workerD.add(slot6);
		ArrayList<WorkerSlot> workerE = new ArrayList<>();
		workerE.add(slot6);
		workerE.add(slot7);
		workerE.add(slot8);
		
		this.am = Mockito.mock(AssignmentMonitor.class);
		Mockito.when(this.am.getRunningComponents(slot1)).thenReturn(assignment1);
		Mockito.when(this.am.getRunningComponents(slot2)).thenReturn(assignment2);
		Mockito.when(this.am.getRunningComponents(slot3)).thenReturn(assignment3);
		Mockito.when(this.am.getRunningComponents(slot4)).thenReturn(assignment4);
		Mockito.when(this.am.getRunningComponents(slot5)).thenReturn(assignment5);
		Mockito.when(this.am.getRunningComponents(slot6)).thenReturn(assignment6);
		Mockito.when(this.am.getRunningComponents(slot7)).thenReturn(assignment7);
		Mockito.when(this.am.getRunningComponents(slot8)).thenReturn(assignment8);
		
		Mockito.when(this.am.getAllocatedWorkers("A")).thenReturn(workerA);
		Mockito.when(this.am.getAllocatedWorkers("B")).thenReturn(workerB);
		Mockito.when(this.am.getAllocatedWorkers("C")).thenReturn(workerC);
		Mockito.when(this.am.getAllocatedWorkers("D")).thenReturn(workerD);
		Mockito.when(this.am.getAllocatedWorkers("E")).thenReturn(workerE);
		
		Mockito.when(this.am.getParallelism("A")).thenReturn(3);
		Mockito.when(this.am.getParallelism("B")).thenReturn(4);
		Mockito.when(this.am.getParallelism("C")).thenReturn(2);
		Mockito.when(this.am.getParallelism("D")).thenReturn(2);
		Mockito.when(this.am.getParallelism("E")).thenReturn(3);
		
		ArrayList<Integer> tasks = new ArrayList<>();
		for(int i = 0; i < 16; i++){
			tasks.add(i);
		}
		
		Mockito.when(this.am.getAllSortedTasks("A")).thenReturn(tasks);
		Mockito.when(this.am.getAllSortedTasks("B")).thenReturn(tasks);
		Mockito.when(this.am.getAllSortedTasks("C")).thenReturn(tasks);
		Mockito.when(this.am.getAllSortedTasks("D")).thenReturn(tasks);
		Mockito.when(this.am.getAllSortedTasks("E")).thenReturn(tasks);
		
		XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
		Mockito.when(parser.getWindowSize()).thenReturn(50);
		Mockito.when(parser.getMonitoringFrequency()).thenReturn(10);
		Mockito.when(parser.getStabilityThreshold()).thenReturn(0.3);
		
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
		
		HashMap<Integer, Double> cpuUsageA1 = new HashMap<>();
		cpuUsageA1.put(10, 15.0);
		cpuUsageA1.put(20, 15.0);
		cpuUsageA1.put(30, 15.0);
		cpuUsageA1.put(40, 15.0);
		cpuUsageA1.put(50, 15.0);
		HashMap<Integer, Double> cpuUsageA2 = new HashMap<>();
		cpuUsageA2.put(10, 20.0);
		cpuUsageA2.put(20, 20.0);
		cpuUsageA2.put(30, 20.0);
		cpuUsageA2.put(40, 20.0);
		cpuUsageA2.put(50, 20.0);
		HashMap<Integer, Double> cpuUsageA3 = new HashMap<>();
		cpuUsageA3.put(10, 15.0);
		cpuUsageA3.put(20, 15.0);
		cpuUsageA3.put(30, 15.0);
		cpuUsageA3.put(40, 15.0);
		cpuUsageA3.put(50, 15.0);
		
		HashMap<Integer, Double> cpuUsageB1 = new HashMap<>();
		cpuUsageB1.put(10, 20.0);
		cpuUsageB1.put(20, 20.0);
		cpuUsageB1.put(30, 20.0);
		cpuUsageB1.put(40, 20.0);
		cpuUsageB1.put(50, 20.0);
		HashMap<Integer, Double> cpuUsageB2 = new HashMap<>();
		cpuUsageB2.put(10, 18.0);
		cpuUsageB2.put(20, 18.0);
		cpuUsageB2.put(30, 18.0);
		cpuUsageB2.put(40, 18.0);
		cpuUsageB2.put(50, 18.0);
		HashMap<Integer, Double> cpuUsageB3 = new HashMap<>();
		cpuUsageB3.put(10, 30.0);
		cpuUsageB3.put(20, 30.0);
		cpuUsageB3.put(30, 30.0);
		cpuUsageB3.put(40, 30.0);
		cpuUsageB3.put(50, 30.0);
		HashMap<Integer, Double> cpuUsageB4 = new HashMap<>();
		cpuUsageB4.put(10, 30.0);
		cpuUsageB4.put(20, 30.0);
		cpuUsageB4.put(30, 30.0);
		cpuUsageB4.put(40, 30.0);
		cpuUsageB4.put(50, 30.0);
		
		HashMap<Integer, Double> cpuUsageC1 = new HashMap<>();
		cpuUsageC1.put(10, 15.0);
		cpuUsageC1.put(20, 15.0);
		cpuUsageC1.put(30, 15.0);
		cpuUsageC1.put(40, 15.0);
		cpuUsageC1.put(50, 15.0);
		HashMap<Integer, Double> cpuUsageC2 = new HashMap<>();
		cpuUsageC2.put(10, 20.0);
		cpuUsageC2.put(20, 20.0);
		cpuUsageC2.put(30, 20.0);
		cpuUsageC2.put(40, 20.0);
		cpuUsageC2.put(50, 20.0);
		
		HashMap<Integer, Double> cpuUsageD1 = new HashMap<>();
		cpuUsageD1.put(10, 25.0);
		cpuUsageD1.put(20, 25.0);
		cpuUsageD1.put(30, 25.0);
		cpuUsageD1.put(40, 25.0);
		cpuUsageD1.put(50, 25.0);
		HashMap<Integer, Double> cpuUsageD2 = new HashMap<>();
		cpuUsageD2.put(10, 40.0);
		cpuUsageD2.put(20, 40.0);
		cpuUsageD2.put(30, 40.0);
		cpuUsageD2.put(40, 40.0);
		cpuUsageD2.put(50, 40.0);
		
		HashMap<Integer, Double> cpuUsageE1 = new HashMap<>();
		cpuUsageE1.put(10, 40.0);
		cpuUsageE1.put(20, 40.0);
		cpuUsageE1.put(30, 40.0);
		cpuUsageE1.put(40, 40.0);
		cpuUsageE1.put(50, 40.0);
		HashMap<Integer, Double> cpuUsageE2 = new HashMap<>();
		cpuUsageE2.put(10, 60.0);
		cpuUsageE2.put(20, 60.0);
		cpuUsageE2.put(30, 60.0);
		cpuUsageE2.put(40, 60.0);
		cpuUsageE2.put(50, 60.0);
		HashMap<Integer, Double> cpuUsageE3 = new HashMap<>();
		cpuUsageE3.put(10, 80.0);
		cpuUsageE3.put(20, 80.0);
		cpuUsageE3.put(30, 80.0);
		cpuUsageE3.put(40, 80.0);
		cpuUsageE3.put(50, 80.0);
		StatStorageManager manager = StatStorageManager.getManager("localhost", "autoscale_test", "root", "");
		
 		this.cm = Mockito.mock(ComponentMonitor.class);
		Mockito.when(this.cm.getParser()).thenReturn(parser);
		Mockito.when(this.cm.getMonitoringFrequency()).thenReturn(10);
		Mockito.when(this.cm.getTimestamp()).thenReturn(50);
		Mockito.when(this.cm.getRegisteredComponents()).thenReturn(components);
		Mockito.when(this.cm.getManager()).thenReturn(manager);
		Mockito.when(this.cm.getStats("A")).thenReturn(statsA);
		Mockito.when(this.cm.getStats("B")).thenReturn(statsB);
		Mockito.when(this.cm.getStats("C")).thenReturn(statsC);
		Mockito.when(this.cm.getStats("D")).thenReturn(statsD);
		Mockito.when(this.cm.getStats("E")).thenReturn(statsE);
		Mockito.when(this.cm.getPendingTuples(explorer)).thenReturn(pending);
		
		Mockito.when(this.cm.getCurrentCpuConstraints(explorer)).thenReturn(cpuConstraints);
		Mockito.when(this.cm.getCpuUsageOnWorker("A", "supervisor1", 6700)).thenReturn(cpuUsageA1);
		Mockito.when(this.cm.getCpuUsageOnWorker("A", "supervisor1", 6701)).thenReturn(cpuUsageA2);
		Mockito.when(this.cm.getCpuUsageOnWorker("A", "supervisor2", 6700)).thenReturn(cpuUsageA3);
		Mockito.when(this.cm.getCpuUsageOnWorker("B", "supervisor1", 6700)).thenReturn(cpuUsageB1);
		Mockito.when(this.cm.getCpuUsageOnWorker("B", "supervisor1", 6701)).thenReturn(cpuUsageB2);
		Mockito.when(this.cm.getCpuUsageOnWorker("B", "supervisor1", 6702)).thenReturn(cpuUsageB3);
		Mockito.when(this.cm.getCpuUsageOnWorker("B", "supervisor1", 6703)).thenReturn(cpuUsageB4);
		Mockito.when(this.cm.getCpuUsageOnWorker("C", "supervisor1", 6702)).thenReturn(cpuUsageC1);
		Mockito.when(this.cm.getCpuUsageOnWorker("C", "supervisor2", 6702)).thenReturn(cpuUsageC2);
		Mockito.when(this.cm.getCpuUsageOnWorker("D", "supervisor1", 6702)).thenReturn(cpuUsageD1);
		Mockito.when(this.cm.getCpuUsageOnWorker("D", "supervisor2", 6701)).thenReturn(cpuUsageD2);
		Mockito.when(this.cm.getCpuUsageOnWorker("E", "supervisor2", 6701)).thenReturn(cpuUsageE1);
		Mockito.when(this.cm.getCpuUsageOnWorker("E", "supervisor2", 6702)).thenReturn(cpuUsageE2);
		Mockito.when(this.cm.getCpuUsageOnWorker("E", "supervisor2", 6703)).thenReturn(cpuUsageE3);
		
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.scale.ScalingManager3#validDegree(java.lang.Integer, java.lang.Double, java.lang.Double, java.lang.Double, java.lang.Double, java.lang.Integer)}.
	 */
	@Test
	public void testValidDegree() {
		ScalingManager3 sm = new ScalingManager3();
		boolean valid1 = sm.validDegree(3, 5000.0, 50.0, 20.0, 0.8, 50);
		boolean valid2 = sm.validDegree(5, 5000.0, 50.0, 20.0, 0.8, 50);
		boolean valid3 = sm.validDegree(6, 5000.0, 50.0, 20.0, 0.8, 50);
		assertTrue(!valid1);
		assertTrue(!valid2);
		assertTrue(valid3);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.scale.ScalingManager3#needScaleOut(java.lang.String)}.
	 */
	@Test
	public void testNeedScaleOut() {
		ScalingManager3 sm = new ScalingManager3();
		sm.initDegrees(cm, am);
		sm.computeEstimInputs(cm, explorer);
		sm.computeEstimMaxCapacities(cm);
		sm.computeUtilCPU(cm, am, explorer);
		
		assertTrue(!sm.needScaleOut("A"));
		assertTrue(sm.needScaleOut("B"));
		assertTrue(sm.needScaleOut("C"));
		assertTrue(!sm.needScaleOut("D"));
		assertTrue(sm.needScaleOut("E"));		
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.scale.ScalingManager3#needScaleIn(java.lang.String, storm.autoscale.scheduler.config.XmlConfigParser)}.
	 */
	@Test
	public void testNeedScaleIn() {
		ScalingManager3 sm = new ScalingManager3();
		sm.initDegrees(cm, am);
		sm.computeEstimInputs(cm, explorer);
		sm.computeEstimMaxCapacities(cm);
		sm.computeUtilCPU(cm, am, explorer);
		
		assertTrue(!sm.needScaleIn("A", cm.getParser(), cm, explorer));
		assertTrue(!sm.needScaleIn("B", cm.getParser(), cm, explorer));
		assertTrue(!sm.needScaleIn("C", cm.getParser(), cm, explorer));
		assertTrue(sm.needScaleIn("D", cm.getParser(), cm, explorer));
		assertTrue(!sm.needScaleIn("E", cm.getParser(), cm, explorer));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.scale.ScalingManager3#getEstimInput(java.lang.String)}.
	 */
	@Test
	public void testGetEstimInput() {
		ScalingManager3 sm = new ScalingManager3();
		sm.computeEstimInputs(cm, explorer);
		assertEquals(3676, sm.getEstimInput("A"), 1.0);
		assertEquals(4198, sm.getEstimInput("B"), 1.0);
		assertEquals(4698, sm.getEstimInput("C"), 1.0);
		assertEquals(2589, sm.getEstimInput("D"), 1.0);
		assertEquals(3207, sm.getEstimInput("E"), 1.0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.scale.ScalingManager3#getEstimMaxCapacity(java.lang.String)}.
	 */
	@Test
	public void testGetEstimMaxCapacity() {
		ScalingManager3 sm = new ScalingManager3();
		sm.computeEstimMaxCapacities(cm);
		
		assertEquals(5000, sm.getEstimMaxCapacity("A"), 1.0);
		assertEquals(1000, sm.getEstimMaxCapacity("B"), 1.0);
		assertEquals(2500, sm.getEstimMaxCapacity("C"), 1.0);
		assertEquals(25000, sm.getEstimMaxCapacity("D"), 1.0);
		assertEquals(1000, sm.getEstimMaxCapacity("E"), 1.0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.scale.ScalingManager3#getUtilCPU(java.lang.String)}.
	 */
	@Test
	public void testGetUtilCPU() {
		ScalingManager3 sm = new ScalingManager3();
		sm.computeUtilCPU(cm, am, explorer);
		
		assertEquals(0.475, sm.getUtilCPU("A"), 0.01);
		assertEquals(0.38, sm.getUtilCPU("B"), 0.01);
		assertEquals(0.23, sm.getUtilCPU("C"), 0.01);
		assertEquals(0.4, sm.getUtilCPU("D"), 0.01);
		assertEquals(0.5, sm.getUtilCPU("E"), 0.01);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.scale.ScalingManager3#getScaleInActions()}.
	 */
	@Test
	public void testGetScaleInActions() {
		ScalingManager3 sm = new ScalingManager3();
		sm.initDegrees(cm, am);
		sm.computeEstimInputs(cm, explorer);
		sm.computeEstimMaxCapacities(cm);
		sm.computeUtilCPU(cm, am, explorer);
		sm.computeScalingActions(cm, am, explorer);
		
		HashMap<String, Integer> expected = new HashMap<>();
		expected.put("D", 1);
		assertEquals(expected, sm.getScaleInActions());
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.scale.ScalingManager3#getScaleOutActions()}.
	 */
	@Test
	public void testGetScaleOutActions() {
		ScalingManager3 sm = new ScalingManager3();
		sm.initDegrees(cm, am);
		sm.computeEstimInputs(cm, explorer);
		sm.computeEstimMaxCapacities(cm);
		sm.computeUtilCPU(cm, am, explorer);
		sm.computeScalingActions(cm, am, explorer);
		
		HashMap<String, Integer> expected = new HashMap<>();
		expected.put("B", 16);
		expected.put("C", 16);
		expected.put("E", 8);
		
		assertEquals(expected, sm.getScaleOutActions());
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.scale.ScalingManager3#getDegree(java.lang.String)}.
	 */
	@Test
	public void testGetDegree() {
		ScalingManager3 sm = new ScalingManager3();
		sm.initDegrees(cm, am);
		
		assertEquals(3, sm.getDegree("A"), 0);
		assertEquals(4, sm.getDegree("B"), 0);
		assertEquals(2, sm.getDegree("C"), 0);
		assertEquals(2, sm.getDegree("D"), 0);
		assertEquals(3, sm.getDegree("E"), 0);
	}
}