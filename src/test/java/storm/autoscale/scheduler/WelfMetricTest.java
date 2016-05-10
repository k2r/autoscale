package storm.autoscale.scheduler;

import java.util.ArrayList;

import org.mockito.Mockito;

import junit.framework.TestCase;
import storm.autoscale.scheduler.metrics.WelfMetric;
import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;

public class WelfMetricTest extends TestCase {

	public final void testEstimatedLatency() {
		ComponentMonitor mockCm = Mockito.mock(ComponentMonitor.class);
		Mockito.when(mockCm.getInputQueueSize("A")).thenReturn(1000.0);
		Mockito.when(mockCm.getAvgLatency("A")).thenReturn(90.0);
		Mockito.when(mockCm.getEstimatedSelectivy("A")).thenReturn(0.55);
		
		Mockito.when(mockCm.getInputQueueSize("C")).thenReturn(1250.0);
		Mockito.when(mockCm.getAvgLatency("C")).thenReturn(130.0);
		Mockito.when(mockCm.getEstimatedSelectivy("C")).thenReturn(0.62);
		
		Mockito.when(mockCm.getInputQueueSize("D")).thenReturn(350.0);
		Mockito.when(mockCm.getAvgLatency("D")).thenReturn(180.0);
		Mockito.when(mockCm.getEstimatedSelectivy("D")).thenReturn(1.0);
		
		Mockito.when(mockCm.getInputQueueSize("F")).thenReturn(425.0);
		Mockito.when(mockCm.getAvgLatency("F")).thenReturn(170.0);
		Mockito.when(mockCm.getEstimatedSelectivy("F")).thenReturn(1.0);
		
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
		ComponentMonitor mockCm = Mockito.mock(ComponentMonitor.class);
		Mockito.when(mockCm.getInputQueueSize("A")).thenReturn(1000.0);
		Mockito.when(mockCm.getAvgLatency("A")).thenReturn(90.0);
		Mockito.when(mockCm.getEstimatedSelectivy("A")).thenReturn(0.55);

		Mockito.when(mockCm.getInputQueueSize("C")).thenReturn(1250.0);
		Mockito.when(mockCm.getAvgLatency("C")).thenReturn(130.0);
		Mockito.when(mockCm.getEstimatedSelectivy("C")).thenReturn(0.62);

		Mockito.when(mockCm.getInputQueueSize("D")).thenReturn(350.0);
		Mockito.when(mockCm.getAvgLatency("D")).thenReturn(180.0);
		Mockito.when(mockCm.getEstimatedSelectivy("D")).thenReturn(1.0);

		Mockito.when(mockCm.getInputQueueSize("F")).thenReturn(425.0);
		Mockito.when(mockCm.getAvgLatency("F")).thenReturn(170.0);
		Mockito.when(mockCm.getEstimatedSelectivy("F")).thenReturn(1.0);

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
