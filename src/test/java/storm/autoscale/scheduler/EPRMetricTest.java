/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.HashMap;

import org.mockito.Mockito;

import junit.framework.TestCase;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;
import storm.autoscale.scheduler.metrics.EPRMetric;
import storm.autoscale.scheduler.modules.TopologyExplorer;

/**
 * @author Roland
 *
 */
public class EPRMetricTest extends TestCase {

	/**
	 * Test method for {@link storm.autoscale.scheduler.metrics.EPRMetric#getEPRInfo(java.lang.String)}.
	 */
	public void testGetEPRInfo() {
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.metrics.EPRMetric#computeEstimatedLoad(java.lang.String)}.
	 */
	public void testComputeEstimatedLoad() {
		HashMap<Integer, Long> inputRecordsIncr = new HashMap<>();
		inputRecordsIncr.put(0, 0L);
		inputRecordsIncr.put(1, 10L);
		inputRecordsIncr.put(2, 20L);
		inputRecordsIncr.put(3, 30L);
		inputRecordsIncr.put(4, 40L);
		inputRecordsIncr.put(5, 50L);
		inputRecordsIncr.put(6, 60L);
		inputRecordsIncr.put(7, 70L);
		inputRecordsIncr.put(8, 80L);
		inputRecordsIncr.put(9, 90L);
		
		HashMap<Integer, Long> executedRecordsIncr = new HashMap<>();
		executedRecordsIncr.put(0, 0L);
		executedRecordsIncr.put(1, 10L);
		executedRecordsIncr.put(2, 20L);
		executedRecordsIncr.put(3, 30L);
		executedRecordsIncr.put(4, 40L);
		executedRecordsIncr.put(5, 50L);
		executedRecordsIncr.put(6, 60L);
		executedRecordsIncr.put(7, 70L);
		executedRecordsIncr.put(8, 80L);
		executedRecordsIncr.put(9, 90L);
		
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
		
		HashMap<Integer, Long> executedRecordsDecr = new HashMap<>();
		executedRecordsDecr.put(0, 190L);
		executedRecordsDecr.put(1, 180L);
		executedRecordsDecr.put(2, 170L);
		executedRecordsDecr.put(3, 160L);
		executedRecordsDecr.put(4, 150L);
		executedRecordsDecr.put(5, 140L);
		executedRecordsDecr.put(6, 130L);
		executedRecordsDecr.put(7, 120L);
		executedRecordsDecr.put(8, 110L);
		executedRecordsDecr.put(9, 100L);
		
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
		
		HashMap<Integer, Long> executedRecordsConst = new HashMap<>();
		executedRecordsConst.put(0, 50L);
		executedRecordsConst.put(1, 50L);
		executedRecordsConst.put(2, 50L);
		executedRecordsConst.put(3, 50L);
		executedRecordsConst.put(4, 50L);
		executedRecordsConst.put(5, 50L);
		executedRecordsConst.put(6, 50L);
		executedRecordsConst.put(7, 50L);
		executedRecordsConst.put(8, 50L);
		executedRecordsConst.put(9, 50L);
		
		ComponentWindowedStats stats1 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats1.getInputRecords()).thenReturn(inputRecordsIncr);
		Mockito.when(stats1.getExecutedRecords()).thenReturn(executedRecordsIncr);
		
		ComponentWindowedStats stats2 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats2.getInputRecords()).thenReturn(inputRecordsDecr);
		Mockito.when(stats2.getExecutedRecords()).thenReturn(executedRecordsDecr);
		
		ComponentWindowedStats stats3 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats3.getInputRecords()).thenReturn(inputRecordsConst);
		Mockito.when(stats3.getExecutedRecords()).thenReturn(executedRecordsConst);
		
		HashMap<String, Long> remainingTuples = new HashMap<>();
		remainingTuples.put("component1", 0L);
		remainingTuples.put("component2", 0L);
		remainingTuples.put("component3", 0L);
		
		ComponentMonitor compMonitor = Mockito.mock(ComponentMonitor.class);
		Mockito.when(compMonitor.getStats("component1")).thenReturn(stats1);
		Mockito.when(compMonitor.getStats("component2")).thenReturn(stats2);
		Mockito.when(compMonitor.getStats("component3")).thenReturn(stats3);
		Mockito.when(compMonitor.getSamplingRate()).thenReturn(1);
		Mockito.when(compMonitor.getFormerRemainingTuples()).thenReturn(remainingTuples);
		
		TopologyExplorer explorer = Mockito.mock(TopologyExplorer.class);
		
		EPRMetric eprMetric = new EPRMetric(explorer, compMonitor);
		assertEquals(1450.0, eprMetric.computeEstimatedLoad("component1"), 0);
		assertEquals(450.0, eprMetric.computeEstimatedLoad("component2"), 0);
		assertEquals(500.0, eprMetric.computeEstimatedLoad("component3"), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.metrics.EPRMetric#computeEstimatedProcessing(java.lang.String)}.
	 */
	public void testComputeEstimatedProcessing() {
		HashMap<Integer, Double> latencyRecordsConst = new HashMap<>();
		latencyRecordsConst.put(0, 20.0);
		latencyRecordsConst.put(1, 20.0);
		latencyRecordsConst.put(2, 20.0);
		latencyRecordsConst.put(3, 20.0);
		latencyRecordsConst.put(4, 20.0);
		latencyRecordsConst.put(5, 20.0);
		latencyRecordsConst.put(6, 20.0);
		latencyRecordsConst.put(7, 20.0);
		latencyRecordsConst.put(8, 20.0);
		latencyRecordsConst.put(9, 20.0);
		
		HashMap<Integer, Double> latencyRecordsIncr = new HashMap<>();
		latencyRecordsIncr.put(0, 150.0);
		latencyRecordsIncr.put(1, 180.0);
		latencyRecordsIncr.put(2, 200.0);
		latencyRecordsIncr.put(3, 180.0);
		latencyRecordsIncr.put(4, 150.0);
		latencyRecordsIncr.put(5, 130.0);
		latencyRecordsIncr.put(6, 90.0);
		latencyRecordsIncr.put(7, 50.0);
		latencyRecordsIncr.put(8, 20.0);
		latencyRecordsIncr.put(9, 10.0);
		
		HashMap<Integer, Double> latencyRecordsDecr = new HashMap<>();
		latencyRecordsDecr.put(0, 10.0);
		latencyRecordsDecr.put(1, 20.0);
		latencyRecordsDecr.put(2, 50.0);
		latencyRecordsDecr.put(3, 90.0);
		latencyRecordsDecr.put(4, 130.0);
		latencyRecordsDecr.put(5, 150.0);
		latencyRecordsDecr.put(6, 180.0);
		latencyRecordsDecr.put(7, 200.0);
		latencyRecordsDecr.put(8, 180.0);
		latencyRecordsDecr.put(9, 150.0);
		
		ComponentWindowedStats stats1 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats1.getAvgLatencyRecords()).thenReturn(latencyRecordsConst);
		
		ComponentWindowedStats stats2 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats2.getAvgLatencyRecords()).thenReturn(latencyRecordsIncr);
		
		ComponentWindowedStats stats3 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats3.getAvgLatencyRecords()).thenReturn(latencyRecordsDecr);
		
		HashMap<String, Long> remainingTuples = new HashMap<>();
		remainingTuples.put("component1", 0L);
		remainingTuples.put("component2", 0L);
		remainingTuples.put("component3", 0L);
		
		ComponentMonitor compMonitor = Mockito.mock(ComponentMonitor.class);
		Mockito.when(compMonitor.getStats("component1")).thenReturn(stats1);
		Mockito.when(compMonitor.getStats("component2")).thenReturn(stats2);
		Mockito.when(compMonitor.getStats("component3")).thenReturn(stats3);
		Mockito.when(compMonitor.getSamplingRate()).thenReturn(1);
		Mockito.when(compMonitor.getFormerRemainingTuples()).thenReturn(remainingTuples);
		
		TopologyExplorer explorer = Mockito.mock(TopologyExplorer.class);
		
		EPRMetric eprMetric = new EPRMetric(explorer, compMonitor);
		assertEquals(500.0, eprMetric.computeEstimatedProcessing("component1"), 0);
		assertEquals(972.0, eprMetric.computeEstimatedProcessing("component2"), 1);
		assertEquals(0.0, eprMetric.computeEstimatedProcessing("component3"), 0);
	}
	

	/**
	 * Test method for {@link storm.autoscale.scheduler.metrics.EPRMetric#compute(java.lang.String)}.
	 */
	public void testCompute() {
		HashMap<Integer, Long> inputRecordsIncr = new HashMap<>();
		inputRecordsIncr.put(0, 0L);
		inputRecordsIncr.put(1, 10L);
		inputRecordsIncr.put(2, 20L);
		inputRecordsIncr.put(3, 30L);
		inputRecordsIncr.put(4, 40L);
		inputRecordsIncr.put(5, 50L);
		inputRecordsIncr.put(6, 60L);
		inputRecordsIncr.put(7, 70L);
		inputRecordsIncr.put(8, 80L);
		inputRecordsIncr.put(9, 90L);
		
		HashMap<Integer, Long> executedRecordsIncr = new HashMap<>();
		executedRecordsIncr.put(0, 0L);
		executedRecordsIncr.put(1, 10L);
		executedRecordsIncr.put(2, 20L);
		executedRecordsIncr.put(3, 30L);
		executedRecordsIncr.put(4, 40L);
		executedRecordsIncr.put(5, 50L);
		executedRecordsIncr.put(6, 60L);
		executedRecordsIncr.put(7, 70L);
		executedRecordsIncr.put(8, 80L);
		executedRecordsIncr.put(9, 90L);
		
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
		
		HashMap<Integer, Long> executedRecordsDecr = new HashMap<>();
		executedRecordsDecr.put(0, 190L);
		executedRecordsDecr.put(1, 180L);
		executedRecordsDecr.put(2, 170L);
		executedRecordsDecr.put(3, 160L);
		executedRecordsDecr.put(4, 150L);
		executedRecordsDecr.put(5, 140L);
		executedRecordsDecr.put(6, 130L);
		executedRecordsDecr.put(7, 120L);
		executedRecordsDecr.put(8, 110L);
		executedRecordsDecr.put(9, 100L);
		
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
		
		HashMap<Integer, Long> executedRecordsConst = new HashMap<>();
		executedRecordsConst.put(0, 50L);
		executedRecordsConst.put(1, 50L);
		executedRecordsConst.put(2, 50L);
		executedRecordsConst.put(3, 50L);
		executedRecordsConst.put(4, 50L);
		executedRecordsConst.put(5, 50L);
		executedRecordsConst.put(6, 50L);
		executedRecordsConst.put(7, 50L);
		executedRecordsConst.put(8, 50L);
		executedRecordsConst.put(9, 50L);
		
		HashMap<Integer, Double> latencyRecordsConst = new HashMap<>();
		latencyRecordsConst.put(0, 20.0);
		latencyRecordsConst.put(1, 20.0);
		latencyRecordsConst.put(2, 20.0);
		latencyRecordsConst.put(3, 20.0);
		latencyRecordsConst.put(4, 20.0);
		latencyRecordsConst.put(5, 20.0);
		latencyRecordsConst.put(6, 20.0);
		latencyRecordsConst.put(7, 20.0);
		latencyRecordsConst.put(8, 20.0);
		latencyRecordsConst.put(9, 20.0);
		
		HashMap<Integer, Double> latencyRecordsIncr = new HashMap<>();
		latencyRecordsIncr.put(0, 150.0);
		latencyRecordsIncr.put(1, 180.0);
		latencyRecordsIncr.put(2, 200.0);
		latencyRecordsIncr.put(3, 180.0);
		latencyRecordsIncr.put(4, 150.0);
		latencyRecordsIncr.put(5, 130.0);
		latencyRecordsIncr.put(6, 90.0);
		latencyRecordsIncr.put(7, 50.0);
		latencyRecordsIncr.put(8, 20.0);
		latencyRecordsIncr.put(9, 10.0);
		
		HashMap<Integer, Double> latencyRecordsDecr = new HashMap<>();
		latencyRecordsDecr.put(0, 10.0);
		latencyRecordsDecr.put(1, 20.0);
		latencyRecordsDecr.put(2, 50.0);
		latencyRecordsDecr.put(3, 90.0);
		latencyRecordsDecr.put(4, 130.0);
		latencyRecordsDecr.put(5, 150.0);
		latencyRecordsDecr.put(6, 180.0);
		latencyRecordsDecr.put(7, 200.0);
		latencyRecordsDecr.put(8, 180.0);
		latencyRecordsDecr.put(9, 150.0);
		
		ComponentWindowedStats stats1 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats1.getInputRecords()).thenReturn(inputRecordsConst);
		Mockito.when(stats1.getExecutedRecords()).thenReturn(executedRecordsConst);
		Mockito.when(stats1.getAvgLatencyRecords()).thenReturn(latencyRecordsConst);
		
		ComponentWindowedStats stats2 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats2.getInputRecords()).thenReturn(inputRecordsConst);
		Mockito.when(stats2.getExecutedRecords()).thenReturn(executedRecordsConst);
		Mockito.when(stats2.getAvgLatencyRecords()).thenReturn(latencyRecordsDecr);
		
		ComponentWindowedStats stats3 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats3.getInputRecords()).thenReturn(inputRecordsConst);
		Mockito.when(stats3.getExecutedRecords()).thenReturn(executedRecordsConst);
		Mockito.when(stats3.getAvgLatencyRecords()).thenReturn(latencyRecordsIncr);
		
		ComponentWindowedStats stats4 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats4.getInputRecords()).thenReturn(inputRecordsDecr);
		Mockito.when(stats4.getExecutedRecords()).thenReturn(executedRecordsDecr);
		Mockito.when(stats4.getAvgLatencyRecords()).thenReturn(latencyRecordsConst);
		
		ComponentWindowedStats stats5 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats5.getInputRecords()).thenReturn(inputRecordsDecr);
		Mockito.when(stats5.getExecutedRecords()).thenReturn(executedRecordsDecr);
		Mockito.when(stats5.getAvgLatencyRecords()).thenReturn(latencyRecordsDecr);
		
		ComponentWindowedStats stats6 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats6.getInputRecords()).thenReturn(inputRecordsDecr);
		Mockito.when(stats6.getExecutedRecords()).thenReturn(executedRecordsDecr);
		Mockito.when(stats6.getAvgLatencyRecords()).thenReturn(latencyRecordsIncr);
		
		ComponentWindowedStats stats7 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats7.getInputRecords()).thenReturn(inputRecordsIncr);
		Mockito.when(stats7.getExecutedRecords()).thenReturn(executedRecordsIncr);
		Mockito.when(stats7.getAvgLatencyRecords()).thenReturn(latencyRecordsConst);
		
		ComponentWindowedStats stats8 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats8.getInputRecords()).thenReturn(inputRecordsIncr);
		Mockito.when(stats8.getExecutedRecords()).thenReturn(executedRecordsIncr);
		Mockito.when(stats8.getAvgLatencyRecords()).thenReturn(latencyRecordsDecr);
		
		ComponentWindowedStats stats9 = Mockito.mock(ComponentWindowedStats.class);
		Mockito.when(stats9.getInputRecords()).thenReturn(inputRecordsIncr);
		Mockito.when(stats9.getExecutedRecords()).thenReturn(executedRecordsIncr);
		Mockito.when(stats9.getAvgLatencyRecords()).thenReturn(latencyRecordsIncr);
		
		HashMap<String, Long> remainingTuples = new HashMap<>();
		remainingTuples.put("component1", 0L);
		remainingTuples.put("component2", 0L);
		remainingTuples.put("component3", 0L);
		remainingTuples.put("component4", 0L);
		remainingTuples.put("component5", 0L);
		remainingTuples.put("component6", 0L);
		remainingTuples.put("component7", 0L);
		remainingTuples.put("component8", 0L);
		remainingTuples.put("component9", 0L);
		
		ComponentMonitor compMonitor = Mockito.mock(ComponentMonitor.class);
		Mockito.when(compMonitor.getStats("component1")).thenReturn(stats1);
		Mockito.when(compMonitor.getStats("component2")).thenReturn(stats2);
		Mockito.when(compMonitor.getStats("component3")).thenReturn(stats3);
		Mockito.when(compMonitor.getStats("component4")).thenReturn(stats4);
		Mockito.when(compMonitor.getStats("component5")).thenReturn(stats5);
		Mockito.when(compMonitor.getStats("component6")).thenReturn(stats6);
		Mockito.when(compMonitor.getStats("component7")).thenReturn(stats7);
		Mockito.when(compMonitor.getStats("component8")).thenReturn(stats8);
		Mockito.when(compMonitor.getStats("component9")).thenReturn(stats9);
		Mockito.when(compMonitor.getSamplingRate()).thenReturn(1);
		Mockito.when(compMonitor.getFormerRemainingTuples()).thenReturn(remainingTuples);
		
		TopologyExplorer explorer = Mockito.mock(TopologyExplorer.class);
		
		EPRMetric eprMetric = new EPRMetric(explorer, compMonitor);
		assertEquals(1, eprMetric.compute("component1"), 0);
		assertEquals(-1, eprMetric.compute("component2"), 0);
		assertEquals(0.51, eprMetric.compute("component3"), 0.01);
		assertEquals(0.9, eprMetric.compute("component4"), 0);
		assertEquals(-1.0, eprMetric.compute("component5"), 0);
		assertEquals(0.46, eprMetric.compute("component6"), 0.01);
		assertEquals(2.9, eprMetric.compute("component7"), 0);
		assertEquals(-1.0, eprMetric.compute("component8"), 0);
		assertEquals(1.49, eprMetric.compute("component9"), 0.01);
	}
}