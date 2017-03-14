/**
 * 
 */
package storm.autoscale.scheduler;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.mockito.Mockito;
import junit.framework.TestCase;
import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.metrics.ActivityMetric;
import storm.autoscale.scheduler.metrics.ImpactMetric;
import storm.autoscale.scheduler.modules.AssignmentMonitor;
import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.ScalingManager;
import storm.autoscale.scheduler.modules.StatStorageManager;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

/**
 * @author Roland
 *
 */
public class ScalingManagerTest extends TestCase {

	Long scaleFactor = 5L;
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.ScalingManager#isInputDecreasing(java.lang.String)}.
	 */
	public void testIsInputDecreasing() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 250L / this.scaleFactor);
		inputRecords1.put(9, 250L / this.scaleFactor);
		inputRecords1.put(8, 260L / this.scaleFactor);
		inputRecords1.put(7, 255L / this.scaleFactor);
		inputRecords1.put(6, 260L / this.scaleFactor);
		inputRecords1.put(5, 265L / this.scaleFactor);
		inputRecords1.put(4, 266L / this.scaleFactor);
		inputRecords1.put(3, 266L / this.scaleFactor);
		inputRecords1.put(2, 270L / this.scaleFactor);
		inputRecords1.put(1, 272L / this.scaleFactor);
	
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(10, 250L / this.scaleFactor);
		inputRecords2.put(9, 260L / this.scaleFactor);
		inputRecords2.put(8, 275L / this.scaleFactor);
		inputRecords2.put(7, 260L / this.scaleFactor);
		inputRecords2.put(6, 280L / this.scaleFactor);
		inputRecords2.put(5, 310L / this.scaleFactor);
		inputRecords2.put(4, 315L / this.scaleFactor);
		inputRecords2.put(3, 350L / this.scaleFactor);
		inputRecords2.put(2, 355L / this.scaleFactor);
		inputRecords2.put(1, 380L / this.scaleFactor);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, null, null, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords2, null, null, null, null, null);
		
		XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
		Mockito.when(parser.getSlopeThreshold()).thenReturn(0.1);
		
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		
		ScalingManager sm = new ScalingManager(cm, parser);
		
		assertEquals(true, sm.isInputDecreasing("component1"));
		assertEquals(true, sm.isInputDecreasing("component2"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.ScalingManager#isInputStable(java.lang.String)}.
	 */
	public void testIsInputStable() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(10, 1350L / this.scaleFactor);
		inputRecords1.put(9, 1300L / this.scaleFactor);
		inputRecords1.put(8, 1330L / this.scaleFactor);
		inputRecords1.put(7, 1340L / this.scaleFactor);
		inputRecords1.put(6, 1340L / this.scaleFactor);
		inputRecords1.put(5, 1350L / this.scaleFactor);
		inputRecords1.put(4, 1350L / this.scaleFactor);
		inputRecords1.put(3, 1320L / this.scaleFactor);
		inputRecords1.put(2, 1345L / this.scaleFactor);
		inputRecords1.put(1, 1320L / this.scaleFactor);
	
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
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, null, null, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords2, null, null, null, null, null);
		
		XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
		Mockito.when(parser.getSlopeThreshold()).thenReturn(0.1);
		
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		
		ScalingManager sm = new ScalingManager(cm, parser);
		
		assertEquals(true, sm.isInputStable("component1"));
		assertEquals(false, sm.isInputStable("component2"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.ScalingManager#isInputIncreasing(java.lang.String)}.
	 */
	public void testIsInputIncreasing() {
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(1, 250L / this.scaleFactor);
		inputRecords1.put(2, 250L / this.scaleFactor);
		inputRecords1.put(3, 260L / this.scaleFactor);
		inputRecords1.put(4, 255L / this.scaleFactor);
		inputRecords1.put(5, 260L / this.scaleFactor);
		inputRecords1.put(6, 265L / this.scaleFactor);
		inputRecords1.put(7, 266L / this.scaleFactor);
		inputRecords1.put(8, 266L / this.scaleFactor);
		inputRecords1.put(9, 270L / this.scaleFactor);
		inputRecords1.put(10, 272L / this.scaleFactor);
	
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(1, 250L / this.scaleFactor);
		inputRecords2.put(2, 260L / this.scaleFactor);
		inputRecords2.put(3, 275L / this.scaleFactor);
		inputRecords2.put(4, 260L / this.scaleFactor);
		inputRecords2.put(5, 280L / this.scaleFactor);
		inputRecords2.put(6, 310L / this.scaleFactor);
		inputRecords2.put(7, 315L / this.scaleFactor);
		inputRecords2.put(8, 350L / this.scaleFactor);
		inputRecords2.put(9, 355L / this.scaleFactor);
		inputRecords2.put(10, 380L / this.scaleFactor);
		
		ComponentWindowedStats cws1 = new ComponentWindowedStats("component1", inputRecords1, null, null, null, null, null);
		ComponentWindowedStats cws2 = new ComponentWindowedStats("component2", inputRecords2, null, null, null, null, null);
		
		XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
		Mockito.when(parser.getSlopeThreshold()).thenReturn(0.1);
		 
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.updateStats(cws1.getId(), cws1);
		cm.updateStats(cws2.getId(), cws2);
		
		ScalingManager sm = new ScalingManager(cm, parser);
		
		assertEquals(true, sm.isInputIncreasing("component1"));
		assertEquals(true, sm.isInputIncreasing("component2"));
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.ScalingManager#buildActionGraph()}.
	 */
	public void testBuildActionGraph() {
		
		XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
		Mockito.when(parser.getWindowSize()).thenReturn(6);
		Mockito.when(parser.getLowActivityThreshold()).thenReturn(0.3);
		Mockito.when(parser.getHighActivityThreshold()).thenReturn(0.8);
		Mockito.when(parser.getSlopeThreshold()).thenReturn(0.2);
		
		TopologyExplorer explorer = Mockito.mock(TopologyExplorer.class);
		Mockito.when(explorer.getTopologyName()).thenReturn("topologyTest");
		ArrayList<Integer> tasks = new ArrayList<>();
		for(int i = 0; i < 10; i++){
			tasks.add(i);
		}
		
		AssignmentMonitor assignMonitor = Mockito.mock(AssignmentMonitor.class);
		Mockito.when(assignMonitor.getAllSortedTasks("component1")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("component2")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("component3")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("component4")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("component5")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("component6")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("component7")).thenReturn(tasks);
		
		StatStorageManager manager = Mockito.mock(StatStorageManager.class);
		Mockito.when(manager.getCurrentTimestamp()).thenReturn(1);
		
		ActivityMetric metric = Mockito.mock(ActivityMetric.class);
		
		Mockito.when(metric.getTopologyExplorer()).thenReturn(explorer);
		Mockito.when(metric.compute("component1")).thenReturn(0.1);
		Mockito.when(metric.compute("component2")).thenReturn(0.1);
		Mockito.when(metric.compute("component3")).thenReturn(0.5);
		Mockito.when(metric.compute("component4")).thenReturn(0.5);
		Mockito.when(metric.compute("component5")).thenReturn(0.9);
		Mockito.when(metric.compute("component6")).thenReturn(0.9);
		Mockito.when(metric.compute("component7")).thenReturn(2.0);
		
		HashMap<String, BigDecimal> lowActivity = new HashMap<>();
		lowActivity.put(ActivityMetric.ACTIVITY, new BigDecimal("0.1"));
		lowActivity.put(ActivityMetric.CAPPERSEC, new BigDecimal("0.0"));
		lowActivity.put(ActivityMetric.ESTIMLOAD, new BigDecimal("0.0"));
		lowActivity.put(ActivityMetric.REMAINING, new BigDecimal("0.0"));
		HashMap<String, BigDecimal> mediumActivity = new HashMap<>();
		mediumActivity.put(ActivityMetric.ACTIVITY, new BigDecimal("0.5"));
		mediumActivity.put(ActivityMetric.CAPPERSEC, new BigDecimal("0.0"));
		mediumActivity.put(ActivityMetric.ESTIMLOAD, new BigDecimal("0.0"));
		mediumActivity.put(ActivityMetric.REMAINING, new BigDecimal("0.0"));
		HashMap<String, BigDecimal> highActivity = new HashMap<>();
		highActivity.put(ActivityMetric.ACTIVITY, new BigDecimal("0.9"));
		highActivity.put(ActivityMetric.CAPPERSEC, new BigDecimal("0.0"));
		highActivity.put(ActivityMetric.ESTIMLOAD, new BigDecimal("0.0"));
		highActivity.put(ActivityMetric.REMAINING, new BigDecimal("0.0"));
		HashMap<String, BigDecimal> criticalActivity = new HashMap<>();
		criticalActivity.put(ActivityMetric.ACTIVITY, new BigDecimal("2.0"));
		criticalActivity.put(ActivityMetric.CAPPERSEC, new BigDecimal("0.0"));
		criticalActivity.put(ActivityMetric.ESTIMLOAD, new BigDecimal("0.0"));
		criticalActivity.put(ActivityMetric.REMAINING, new BigDecimal("0.0"));
	
		Mockito.when(metric.getActivityInfo("component1")).thenReturn(lowActivity);
		Mockito.when(metric.getActivityInfo("component2")).thenReturn(lowActivity);
		Mockito.when(metric.getActivityInfo("component3")).thenReturn(mediumActivity);
		Mockito.when(metric.getActivityInfo("component4")).thenReturn(mediumActivity);
		Mockito.when(metric.getActivityInfo("component5")).thenReturn(highActivity);
		Mockito.when(metric.getActivityInfo("component6")).thenReturn(highActivity);
		Mockito.when(metric.getActivityInfo("component7")).thenReturn(criticalActivity);
		
		HashMap<String, Integer> degrees = new HashMap<>();
		degrees.put("component1", 4);
		degrees.put("component2", 4);
		degrees.put("component3", 4);
		degrees.put("component4", 4);
		degrees.put("component5", 4);
		degrees.put("component6", 4);
		degrees.put("component7", 4);
		
		HashMap<Integer, Long> inputRecordsIncr = new HashMap<>();
		inputRecordsIncr.put(0, 100L);
		inputRecordsIncr.put(1, 110L);
		inputRecordsIncr.put(2, 120L);
		inputRecordsIncr.put(3, 130L);
		inputRecordsIncr.put(4, 140L);
		inputRecordsIncr.put(5, 150L);
		inputRecordsIncr.put(6, 160L);
		inputRecordsIncr.put(7, 170L);
		inputRecordsIncr.put(8, 180L);
		inputRecordsIncr.put(9, 190L);
		
		HashMap<Integer, Long> inputRecordsDecr = new HashMap<>();
		inputRecordsDecr.put(0, 190L);
		inputRecordsDecr.put(1, 180L);
		inputRecordsDecr.put(2, 170L);
		inputRecordsDecr.put(3, 160L);
		inputRecordsDecr.put(4, 150L);
		inputRecordsDecr.put(5, 140L);
		inputRecordsDecr.put(6, 130L);
		inputRecordsDecr.put(7, 120L);
		inputRecordsDecr.put(8, 110L);
		inputRecordsDecr.put(9, 100L);
		
		HashMap<Integer, Long> inputRecordsConst = new HashMap<>();
		inputRecordsConst.put(0, 50L);
		inputRecordsConst.put(1, 50L);
		inputRecordsConst.put(2, 50L);
		inputRecordsConst.put(3, 50L);
		inputRecordsConst.put(4, 50L);
		inputRecordsConst.put(5, 50L);
		inputRecordsConst.put(6, 50L);
		inputRecordsConst.put(7, 50L);
		inputRecordsConst.put(8, 50L);
		inputRecordsConst.put(9, 50L);
		
		HashMap<Integer, Long> mockLongRecords = new HashMap<>();
		mockLongRecords.put(0, 0L);
		HashMap<Integer, Double> mockDoubleRecords = new HashMap<>();
		mockDoubleRecords.put(0, 0.0);
		
		ComponentWindowedStats cwsIncr = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(cwsIncr.getInputRecords()).thenReturn(inputRecordsIncr);
		Mockito.when(cwsIncr.getExecutedRecords()).thenReturn(mockLongRecords);
		Mockito.when(cwsIncr.getOutputRecords()).thenReturn(mockLongRecords);
		Mockito.when(cwsIncr.getAvgLatencyRecords()).thenReturn(mockDoubleRecords);
		Mockito.when(cwsIncr.getSelectivityRecords()).thenReturn(mockDoubleRecords);
		Mockito.when(cwsIncr.hasRecords()).thenReturn(true);
		
		ComponentWindowedStats cwsDecr = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(cwsDecr.getInputRecords()).thenReturn(inputRecordsDecr);
		Mockito.when(cwsDecr.getExecutedRecords()).thenReturn(mockLongRecords);
		Mockito.when(cwsDecr.getOutputRecords()).thenReturn(mockLongRecords);
		Mockito.when(cwsDecr.getAvgLatencyRecords()).thenReturn(mockDoubleRecords);
		Mockito.when(cwsDecr.getSelectivityRecords()).thenReturn(mockDoubleRecords);
		Mockito.when(cwsDecr.hasRecords()).thenReturn(true);
		
		ComponentWindowedStats cwsConst = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(cwsConst.getInputRecords()).thenReturn(inputRecordsConst);
		Mockito.when(cwsConst.getExecutedRecords()).thenReturn(mockLongRecords);
		Mockito.when(cwsConst.getOutputRecords()).thenReturn(mockLongRecords);
		Mockito.when(cwsConst.getAvgLatencyRecords()).thenReturn(mockDoubleRecords);
		Mockito.when(cwsConst.getSelectivityRecords()).thenReturn(mockDoubleRecords);
		Mockito.when(cwsConst.hasRecords()).thenReturn(true);
		
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.setManager(manager);
		cm.updateStats("component1", cwsDecr);
		cm.updateStats("component2", cwsIncr);
		cm.updateStats("component3", cwsDecr);
		cm.updateStats("component4", cwsConst);
		cm.updateStats("component5", cwsDecr);
		cm.updateStats("component6", cwsIncr);
		cm.updateStats("component7", cwsConst);
		
		ScalingManager sm = new ScalingManager(cm, parser);
		
		sm.setDegrees(degrees);
		
		sm.buildActionGraph(metric, assignMonitor);
		
		HashMap<String, Integer> expectedScaleIn = new HashMap<>();
		expectedScaleIn.put("component1", 1);
		
		HashMap<String, Integer> expectedScaleOut = new HashMap<>();
		expectedScaleOut.put("component6", 5);
		expectedScaleOut.put("component7", 8);
		
		assertEquals(expectedScaleIn, sm.getScaleInActions());
		assertEquals(expectedScaleOut, sm.getScaleOutActions());
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.ScalingManager#autoscaleAlgorithm()}.
	 */
	public void testAutoscaleAlgorithm(){
		XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
		
		TopologyExplorer explorer = Mockito.mock(TopologyExplorer.class);
		ArrayList<String> childrenA = new ArrayList<>();
		childrenA.add("B");
		childrenA.add("C");
		ArrayList<String> childrenB = new ArrayList<>();
		childrenB.add("D");
		childrenB.add("E");
		ArrayList<String> childrenC = new ArrayList<>();
		childrenB.add("E");
		ArrayList<String> childrenE = new ArrayList<>();
		childrenB.add("F");
		
		Mockito.when(explorer.getChildren("A")).thenReturn(childrenA);
		Mockito.when(explorer.getChildren("B")).thenReturn(childrenB);
		Mockito.when(explorer.getChildren("C")).thenReturn(childrenC);
		Mockito.when(explorer.getChildren("D")).thenReturn(new ArrayList<String>());
		Mockito.when(explorer.getChildren("E")).thenReturn(childrenE);
		Mockito.when(explorer.getChildren("F")).thenReturn(new ArrayList<String>());
		
		HashSet<String> ancestors = new HashSet<>();
		ancestors.add("A");
		
		HashMap<String, Integer> degrees = new HashMap<>();
		degrees.put("A", 4);
		degrees.put("B", 4);
		degrees.put("C", 4);
		degrees.put("D", 4);
		degrees.put("E", 4);
		degrees.put("F", 4);
		degrees.put("G", 4);
		
		HashMap<String, Integer> nothingActions = new HashMap<>();
		HashMap<String, Integer> scaleInActions = new HashMap<>();
		HashMap<String, Integer> scaleOutActions = new HashMap<>();

		nothingActions.put("A", 4);
		nothingActions.put("C", 4);
		nothingActions.put("F", 4);
		scaleInActions.put("D", 1);
		scaleOutActions.put("B", 8);
		scaleOutActions.put("E", 6);
		
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		
		ScalingManager sm = new ScalingManager(cm, parser);
		
		sm.setDegrees(degrees);
		sm.setScaleInActions(scaleInActions);
		sm.setNothingActions(nothingActions);
		sm.setScaleOutActions(scaleOutActions);
		sm.autoscaleAlgorithm(ancestors, explorer);
		
		HashMap<String, Integer> expectedNothing = new HashMap<>();
		HashMap<String, Integer> expectedScaleIn = new HashMap<>();
		HashMap<String, Integer> expectedScaleOut = new HashMap<>();
		expectedNothing.put("A", 4);
		expectedNothing.put("C", 4);
		expectedNothing.put("D", 4);
		expectedScaleOut.put("B", 8);
		expectedScaleOut.put("E", 7);
		expectedScaleOut.put("F", 5);
		
		assertEquals(expectedNothing, sm.getNothingActions());
		assertEquals(expectedScaleIn, sm.getScaleInActions());
		assertEquals(expectedScaleOut, sm.getScaleOutActions());
	}
	
	/**
	 * Test method for {@link storm.autoscale.scheduler.modules.ScalingManager#autoscaleAlgorithmWithImpact()}.
	 */
	public void testAutoscaleAlgorithmWithImpact(){
		XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
		
		TopologyExplorer explorer = Mockito.mock(TopologyExplorer.class);
		ArrayList<String> childrenA = new ArrayList<>();
		childrenA.add("B");
		childrenA.add("C");
		ArrayList<String> childrenB = new ArrayList<>();
		childrenB.add("D");
		childrenB.add("E");
		ArrayList<String> childrenC = new ArrayList<>();
		childrenB.add("E");
		ArrayList<String> childrenE = new ArrayList<>();
		childrenB.add("F");
		
		Mockito.when(explorer.getChildren("A")).thenReturn(childrenA);
		Mockito.when(explorer.getChildren("B")).thenReturn(childrenB);
		Mockito.when(explorer.getChildren("C")).thenReturn(childrenC);
		Mockito.when(explorer.getChildren("D")).thenReturn(new ArrayList<String>());
		Mockito.when(explorer.getChildren("E")).thenReturn(childrenE);
		Mockito.when(explorer.getChildren("F")).thenReturn(new ArrayList<String>());
		
		HashSet<String> ancestors = new HashSet<>();
		ancestors.add("A");
		
		HashMap<String, Integer> degrees = new HashMap<>();
		degrees.put("A", 4);
		degrees.put("B", 4);
		degrees.put("C", 4);
		degrees.put("D", 4);
		degrees.put("E", 4);
		degrees.put("F", 4);
		degrees.put("G", 4);
		
		HashMap<String, Double> capacities = new HashMap<>();
		capacities.put("A", 200.0);
		capacities.put("B", 40.0);
		capacities.put("C", 100.0);
		capacities.put("D", 150.0);
		capacities.put("E", 60.0);
		capacities.put("F", 50.0);
		capacities.put("G", 50.0);
		
		HashMap<String, Integer> nothingActions = new HashMap<>();
		HashMap<String, Integer> scaleInActions = new HashMap<>();
		HashMap<String, Integer> scaleOutActions = new HashMap<>();

		nothingActions.put("A", 4);
		nothingActions.put("C", 4);
		nothingActions.put("F", 4);
		scaleInActions.put("D", 1);
		scaleOutActions.put("B", 8);
		scaleOutActions.put("E", 6);
		
		ArrayList<Integer> tasks = new ArrayList<>();
		for(int i = 0; i < 10; i++){
			tasks.add(i);
		}
		
		AssignmentMonitor assignMonitor = Mockito.mock(AssignmentMonitor.class);
		Mockito.when(assignMonitor.getAllSortedTasks("A")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("B")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("C")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("D")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("E")).thenReturn(tasks);
		Mockito.when(assignMonitor.getAllSortedTasks("F")).thenReturn(tasks);
		
		HashMap<String, Integer> impDegrees = new HashMap<>();
		impDegrees.put("A", 1);
		impDegrees.put("B", 3);
		impDegrees.put("C", 3);
		impDegrees.put("D", 4);
		impDegrees.put("E", 7);
		impDegrees.put("F", 4);
	
		ImpactMetric impact = Mockito.mock(ImpactMetric.class);
		Mockito.when(impact.getImpactDegrees()).thenReturn(impDegrees);
		Mockito.when(impact.compute("A")).thenReturn(0.0);
		Mockito.when(impact.compute("B")).thenReturn(70.0);
		Mockito.when(impact.compute("C")).thenReturn(70.0);
		Mockito.when(impact.compute("D")).thenReturn(90.0);
		Mockito.when(impact.compute("E")).thenReturn(160.0);
		Mockito.when(impact.compute("F")).thenReturn(84.0);
		
		ComponentMonitor cm = new ComponentMonitor(parser, null, null);
		cm.setParser(parser);
		
		ScalingManager sm = new ScalingManager(cm, parser);
		sm.setDegrees(degrees);
		sm.setNothingActions(nothingActions);
		sm.setScaleInActions(scaleInActions);
		sm.setScaleOutActions(scaleOutActions);
		sm.setCapacities(capacities);
		
		sm.autoscaleAlgorithmWithImpact(impact, ancestors, explorer, assignMonitor);
		HashMap<String, Integer> expectedNothing = new HashMap<>();
		HashMap<String, Integer> expectedScaleIn = new HashMap<>();
		HashMap<String, Integer> expectedScaleOut = new HashMap<>();
		expectedNothing.put("A", 4);
		expectedNothing.put("C", 4);
		expectedNothing.put("D", 4);
		expectedNothing.put("F", 4);
		expectedScaleOut.put("B", 8);
		expectedScaleOut.put("E", 7);
		
		
		assertEquals(expectedNothing, sm.getNothingActions());
		assertEquals(expectedScaleIn, sm.getScaleInActions());
		assertEquals(expectedScaleOut, sm.getScaleOutActions());		
	}
}
