package storm.autoscale.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import org.mockito.Mockito;

import junit.framework.TestCase;
import storm.autoscale.scheduler.metrics.WelfMetric;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

public class WelfMetricTest extends TestCase {

	public final void testEstimatedLatency() {
		ComponentWindowedStats statsA = Mockito.mock(ComponentWindowedStats.class);
		ComponentWindowedStats statsC = Mockito.mock(ComponentWindowedStats.class);
		ComponentWindowedStats statsD = Mockito.mock(ComponentWindowedStats.class);
		ComponentWindowedStats statsF = Mockito.mock(ComponentWindowedStats.class);
		
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(0, 1000L);
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(0, 1250L);
		HashMap<Integer, Long> inputRecords3 = new HashMap<>();
		inputRecords3.put(0, 350L);
		HashMap<Integer, Long> inputRecords4 = new HashMap<>();
		inputRecords4.put(0, 425L);
		
		HashMap<Integer, Double> avgLatencyRecords1 = new HashMap<>();
		avgLatencyRecords1.put(0, 90.0);
		HashMap<Integer, Double> avgLatencyRecords2 = new HashMap<>();
		avgLatencyRecords2.put(0, 130.0);
		HashMap<Integer, Double> avgLatencyRecords3 = new HashMap<>();
		avgLatencyRecords3.put(0, 180.0);
		HashMap<Integer, Double> avgLatencyRecords4 = new HashMap<>();
		avgLatencyRecords4.put(0, 170.0);
		
		HashMap<Integer, Double> selectivityRecords1 = new HashMap<>();
		selectivityRecords1.put(0, 0.55);
		HashMap<Integer, Double> selectivityRecords2 = new HashMap<>();
		selectivityRecords2.put(0, 0.62);
		HashMap<Integer, Double> selectivityRecords3 = new HashMap<>();
		selectivityRecords3.put(0, 1.0);
		HashMap<Integer, Double> selectivityRecords4 = new HashMap<>();
		selectivityRecords4.put(0, 1.0);
		
		Mockito.when(statsA.getInputRecords()).thenReturn(inputRecords1);
		Mockito.when(statsA.getAvgLatencyRecords()).thenReturn(avgLatencyRecords1);
		Mockito.when(statsA.getSelectivityRecords()).thenReturn(selectivityRecords1);
		
		Mockito.when(statsC.getInputRecords()).thenReturn(inputRecords2);
		Mockito.when(statsC.getAvgLatencyRecords()).thenReturn(avgLatencyRecords2);
		Mockito.when(statsC.getSelectivityRecords()).thenReturn(selectivityRecords2);
		
		Mockito.when(statsD.getInputRecords()).thenReturn(inputRecords3);
		Mockito.when(statsD.getAvgLatencyRecords()).thenReturn(avgLatencyRecords3);
		Mockito.when(statsD.getSelectivityRecords()).thenReturn(selectivityRecords3);
		
		Mockito.when(statsF.getInputRecords()).thenReturn(inputRecords4);
		Mockito.when(statsF.getAvgLatencyRecords()).thenReturn(avgLatencyRecords4);
		Mockito.when(statsF.getSelectivityRecords()).thenReturn(selectivityRecords4);
		
		ComponentMonitor mockCm = Mockito.mock(ComponentMonitor.class);
		Mockito.when(mockCm.getStats("A")).thenReturn(statsA);
		Mockito.when(mockCm.getStats("C")).thenReturn(statsC);
		Mockito.when(mockCm.getStats("D")).thenReturn(statsD);
		Mockito.when(mockCm.getStats("F")).thenReturn(statsF);
		
		TopologyExplorer mockTe = Mockito.mock(TopologyExplorer.class);
		
		WelfMetric wm = new WelfMetric(mockTe, mockCm);
		
		Double estimatedLatencyA = wm.estimatedLatency("A");
		Double estimatedLatencyC = wm.estimatedLatency("C");
		Double estimatedLatencyD = wm.estimatedLatency("D");
		Double estimatedLatencyF = wm.estimatedLatency("F");
		
		assertEquals(49500.0, estimatedLatencyA);
		assertEquals(100750.0, estimatedLatencyC);
		assertEquals(63000.0, estimatedLatencyD);
		assertEquals(72250.0, estimatedLatencyF);
	}

	public final void testCompute() {
		ComponentWindowedStats statsA = Mockito.mock(ComponentWindowedStats.class);
		ComponentWindowedStats statsC = Mockito.mock(ComponentWindowedStats.class);
		ComponentWindowedStats statsD = Mockito.mock(ComponentWindowedStats.class);
		ComponentWindowedStats statsF = Mockito.mock(ComponentWindowedStats.class);
		
		HashMap<Integer, Long> inputRecords1 = new HashMap<>();
		inputRecords1.put(0, 1000L);
		HashMap<Integer, Long> inputRecords2 = new HashMap<>();
		inputRecords2.put(0, 1250L);
		HashMap<Integer, Long> inputRecords3 = new HashMap<>();
		inputRecords3.put(0, 350L);
		HashMap<Integer, Long> inputRecords4 = new HashMap<>();
		inputRecords4.put(0, 425L);
		
		HashMap<Integer, Double> avgLatencyRecords1 = new HashMap<>();
		avgLatencyRecords1.put(0, 90.0);
		HashMap<Integer, Double> avgLatencyRecords2 = new HashMap<>();
		avgLatencyRecords2.put(0, 130.0);
		HashMap<Integer, Double> avgLatencyRecords3 = new HashMap<>();
		avgLatencyRecords3.put(0, 180.0);
		HashMap<Integer, Double> avgLatencyRecords4 = new HashMap<>();
		avgLatencyRecords4.put(0, 170.0);
		
		HashMap<Integer, Double> selectivityRecords1 = new HashMap<>();
		selectivityRecords1.put(0, 0.55);
		HashMap<Integer, Double> selectivityRecords2 = new HashMap<>();
		selectivityRecords2.put(0, 0.62);
		HashMap<Integer, Double> selectivityRecords3 = new HashMap<>();
		selectivityRecords3.put(0, 1.0);
		HashMap<Integer, Double> selectivityRecords4 = new HashMap<>();
		selectivityRecords4.put(0, 1.0);
		
		Mockito.when(statsA.getInputRecords()).thenReturn(inputRecords1);
		Mockito.when(statsA.getAvgLatencyRecords()).thenReturn(avgLatencyRecords1);
		Mockito.when(statsA.getSelectivityRecords()).thenReturn(selectivityRecords1);
		
		Mockito.when(statsC.getInputRecords()).thenReturn(inputRecords2);
		Mockito.when(statsC.getAvgLatencyRecords()).thenReturn(avgLatencyRecords2);
		Mockito.when(statsC.getSelectivityRecords()).thenReturn(selectivityRecords2);
		
		Mockito.when(statsD.getInputRecords()).thenReturn(inputRecords3);
		Mockito.when(statsD.getAvgLatencyRecords()).thenReturn(avgLatencyRecords3);
		Mockito.when(statsD.getSelectivityRecords()).thenReturn(selectivityRecords3);
		
		Mockito.when(statsF.getInputRecords()).thenReturn(inputRecords4);
		Mockito.when(statsF.getAvgLatencyRecords()).thenReturn(avgLatencyRecords4);
		Mockito.when(statsF.getSelectivityRecords()).thenReturn(selectivityRecords4);
		
		ComponentMonitor mockCm = Mockito.mock(ComponentMonitor.class);
		Mockito.when(mockCm.getStats("A")).thenReturn(statsA);
		Mockito.when(mockCm.getStats("C")).thenReturn(statsC);
		Mockito.when(mockCm.getStats("D")).thenReturn(statsD);
		Mockito.when(mockCm.getStats("F")).thenReturn(statsF);

		TopologyExplorer mockTe = Mockito.mock(TopologyExplorer.class);

		ArrayList<String> childrenA = new ArrayList<>();
		childrenA.add("C");

		ArrayList<String> childrenC = new ArrayList<>();
		childrenC.add("D");
		childrenC.add("F");

		Mockito.when(mockTe.getChildren("A")).thenReturn(childrenA);
		Mockito.when(mockTe.getChildren("C")).thenReturn(childrenC);
		Mockito.when(mockTe.getChildren("D")).thenReturn(new ArrayList<>());
		Mockito.when(mockTe.getChildren("F")).thenReturn(new ArrayList<>());

		WelfMetric wm = new WelfMetric(mockTe, mockCm);
		Double welfA = wm.compute("A");
		assertEquals(0.17, welfA, 0.05);
	}
}