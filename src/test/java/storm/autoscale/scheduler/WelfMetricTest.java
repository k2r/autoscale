package storm.autoscale.scheduler;

import java.util.ArrayList;

import org.mockito.Mockito;

import junit.framework.TestCase;
import storm.autoscale.scheduler.metrics.WelfMetric;
import storm.autoscale.scheduler.modules.stats.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentStats;

public class WelfMetricTest extends TestCase {

	public final void testEstimatedLatency() {
		ComponentStats statsA = Mockito.mock(ComponentStats.class);
		ComponentStats statsC = Mockito.mock(ComponentStats.class);
		ComponentStats statsD = Mockito.mock(ComponentStats.class);
		ComponentStats statsF = Mockito.mock(ComponentStats.class);
		
		Mockito.when(statsA.getNbInputs()).thenReturn(1000.0);
		Mockito.when(statsA.getAvgLatency()).thenReturn(90.0);
		Mockito.when(statsA.getSelectivity()).thenReturn(0.55);
		
		Mockito.when(statsC.getNbInputs()).thenReturn(1250.0);
		Mockito.when(statsC.getAvgLatency()).thenReturn(130.0);
		Mockito.when(statsC.getSelectivity()).thenReturn(0.62);
		
		Mockito.when(statsD.getNbInputs()).thenReturn(350.0);
		Mockito.when(statsD.getAvgLatency()).thenReturn(180.0);
		Mockito.when(statsD.getSelectivity()).thenReturn(1.0);
		
		Mockito.when(statsF.getNbInputs()).thenReturn(425.0);
		Mockito.when(statsF.getAvgLatency()).thenReturn(170.0);
		Mockito.when(statsF.getSelectivity()).thenReturn(1.0);
		
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
		ComponentStats statsA = Mockito.mock(ComponentStats.class);
		ComponentStats statsC = Mockito.mock(ComponentStats.class);
		ComponentStats statsD = Mockito.mock(ComponentStats.class);
		ComponentStats statsF = Mockito.mock(ComponentStats.class);
		
		Mockito.when(statsA.getNbInputs()).thenReturn(1000.0);
		Mockito.when(statsA.getAvgLatency()).thenReturn(90.0);
		Mockito.when(statsA.getSelectivity()).thenReturn(0.55);
		
		Mockito.when(statsC.getNbInputs()).thenReturn(1250.0);
		Mockito.when(statsC.getAvgLatency()).thenReturn(130.0);
		Mockito.when(statsC.getSelectivity()).thenReturn(0.62);
		
		Mockito.when(statsD.getNbInputs()).thenReturn(350.0);
		Mockito.when(statsD.getAvgLatency()).thenReturn(180.0);
		Mockito.when(statsD.getSelectivity()).thenReturn(1.0);
		
		Mockito.when(statsF.getNbInputs()).thenReturn(425.0);
		Mockito.when(statsF.getAvgLatency()).thenReturn(170.0);
		Mockito.when(statsF.getSelectivity()).thenReturn(1.0);
		
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