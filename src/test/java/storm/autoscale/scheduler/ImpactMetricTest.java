/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import org.mockito.Mockito;

import junit.framework.TestCase;
import storm.autoscale.scheduler.config.XmlConfigParser;
import storm.autoscale.scheduler.metrics.ImpactMetric;
import storm.autoscale.scheduler.modules.ComponentMonitor;
import storm.autoscale.scheduler.modules.TopologyExplorer;
import storm.autoscale.scheduler.modules.stats.ComponentWindowedStats;

/**
 * @author Roland
 *
 */
public class ImpactMetricTest extends TestCase {

	/**
	 * Test method for {@link storm.autoscale.scheduler.metrics.ImpactMetric#getImpactDegrees()}.
	 */
	public void testGetImpactDegrees() {
		XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
		Mockito.when(parser.getWindowSize()).thenReturn(10);
		Mockito.when(parser.getMonitoringFrequency()).thenReturn(1);
		
		TopologyExplorer explorer = Mockito.mock(TopologyExplorer.class);
		
		ArrayList<String> parentsB = new ArrayList<>();
		parentsB.add("A");
		ArrayList<String> parentsC = new ArrayList<>();
		parentsC.add("A");
		ArrayList<String> parentsD = new ArrayList<>();
		parentsD.add("B");
		ArrayList<String> parentsE = new ArrayList<>();
		parentsE.add("B");
		parentsE.add("C");
		ArrayList<String> parentsF = new ArrayList<>();
		parentsF.add("E");
		
		Mockito.when(explorer.getParents("A")).thenReturn(new ArrayList<String>());
		Mockito.when(explorer.getParents("B")).thenReturn(parentsB);
		Mockito.when(explorer.getParents("C")).thenReturn(parentsC);
		Mockito.when(explorer.getParents("D")).thenReturn(parentsD);
		Mockito.when(explorer.getParents("E")).thenReturn(parentsE);
		Mockito.when(explorer.getParents("F")).thenReturn(parentsF);
		
		ComponentMonitor cm = Mockito.mock(ComponentMonitor.class);
		
		Mockito.when(cm.getParser()).thenReturn(parser);
		
		Mockito.when(cm.getCurrentDegree("A")).thenReturn(4);
		Mockito.when(cm.getCurrentDegree("B")).thenReturn(4);
		Mockito.when(cm.getCurrentDegree("C")).thenReturn(4);
		Mockito.when(cm.getCurrentDegree("D")).thenReturn(4);
		Mockito.when(cm.getCurrentDegree("E")).thenReturn(4);
		Mockito.when(cm.getCurrentDegree("F")).thenReturn(4);
		
		Mockito.when(cm.getEstimatedLoad("A")).thenReturn(70.0);
		Mockito.when(cm.getEstimatedLoad("B")).thenReturn(300.0);
		Mockito.when(cm.getEstimatedLoad("C")).thenReturn(70.0);
		Mockito.when(cm.getEstimatedLoad("D")).thenReturn(10.0);
		Mockito.when(cm.getEstimatedLoad("E")).thenReturn(120.0);
		Mockito.when(cm.getEstimatedLoad("F")).thenReturn(60.0);
		
		Mockito.when(cm.getCapacity("A")).thenReturn(10.0);
		Mockito.when(cm.getCapacity("B")).thenReturn(10.0);
		Mockito.when(cm.getCapacity("C")).thenReturn(10.0);
		Mockito.when(cm.getCapacity("D")).thenReturn(10.0);
		Mockito.when(cm.getCapacity("E")).thenReturn(10.0);
		Mockito.when(cm.getCapacity("F")).thenReturn(10.0);
		
		ComponentWindowedStats cwsLowSelect = Mockito.mock(ComponentWindowedStats.class);
		HashMap<Integer, Double> lowSelect = new HashMap<>();
		lowSelect.put(0, 0.3);
		Mockito.when(cwsLowSelect.getSelectivityRecords()).thenReturn(lowSelect);
		
		ComponentWindowedStats cwsMediumSelect = Mockito.mock(ComponentWindowedStats.class);
		HashMap<Integer, Double> mediumSelect = new HashMap<>();
		mediumSelect.put(0, 0.7);
		Mockito.when(cwsMediumSelect.getSelectivityRecords()).thenReturn(mediumSelect);
		
		ComponentWindowedStats cwsHighSelect = Mockito.mock(ComponentWindowedStats.class);
		HashMap<Integer, Double> highSelect = new HashMap<>();
		highSelect.put(0, 1.0);
		Mockito.when(cwsHighSelect.getSelectivityRecords()).thenReturn(highSelect);
		
		Mockito.when(cm.getStats("A")).thenReturn(cwsHighSelect);
		Mockito.when(cm.getStats("B")).thenReturn(cwsLowSelect);
		Mockito.when(cm.getStats("C")).thenReturn(cwsHighSelect);
		Mockito.when(cm.getStats("D")).thenReturn(cwsHighSelect);
		Mockito.when(cm.getStats("E")).thenReturn(cwsMediumSelect);
		Mockito.when(cm.getStats("F")).thenReturn(cwsHighSelect);
		
		ImpactMetric impactMetric = new ImpactMetric(cm, explorer);
		impactMetric.compute("A");
		impactMetric.compute("B");
		impactMetric.compute("C");
		impactMetric.compute("D");
		impactMetric.compute("E");
		impactMetric.compute("F");
		
		HashMap<String, Integer> impDegrees = impactMetric.getImpactDegrees();
		
		assertEquals(1, impDegrees.get("A"), 0);
		assertEquals(3, impDegrees.get("B"), 0);
		assertEquals(3, impDegrees.get("C"), 0);
		assertEquals(4, impDegrees.get("D"), 0);
		assertEquals(7, impDegrees.get("E"), 0);
		assertEquals(4, impDegrees.get("F"), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.metrics.ImpactMetric#compute(java.lang.String)}.
	 */
	public void testCompute() {
		XmlConfigParser parser = Mockito.mock(XmlConfigParser.class);
		Mockito.when(parser.getWindowSize()).thenReturn(10);
		Mockito.when(parser.getMonitoringFrequency()).thenReturn(1);
		
		TopologyExplorer explorer = Mockito.mock(TopologyExplorer.class);
		
		ArrayList<String> parentsB = new ArrayList<>();
		parentsB.add("A");
		ArrayList<String> parentsC = new ArrayList<>();
		parentsC.add("A");
		ArrayList<String> parentsD = new ArrayList<>();
		parentsD.add("B");
		ArrayList<String> parentsE = new ArrayList<>();
		parentsE.add("B");
		parentsE.add("C");
		ArrayList<String> parentsF = new ArrayList<>();
		parentsF.add("E");
		
		Mockito.when(explorer.getParents("A")).thenReturn(new ArrayList<String>());
		Mockito.when(explorer.getParents("B")).thenReturn(parentsB);
		Mockito.when(explorer.getParents("C")).thenReturn(parentsC);
		Mockito.when(explorer.getParents("D")).thenReturn(parentsD);
		Mockito.when(explorer.getParents("E")).thenReturn(parentsE);
		Mockito.when(explorer.getParents("F")).thenReturn(parentsF);
		
		ComponentMonitor cm = Mockito.mock(ComponentMonitor.class);
		
		Mockito.when(cm.getParser()).thenReturn(parser);
		
		Mockito.when(cm.getEstimatedLoad("A")).thenReturn(70.0);
		Mockito.when(cm.getEstimatedLoad("B")).thenReturn(300.0);
		Mockito.when(cm.getEstimatedLoad("C")).thenReturn(70.0);
		Mockito.when(cm.getEstimatedLoad("D")).thenReturn(10.0);
		Mockito.when(cm.getEstimatedLoad("E")).thenReturn(120.0);
		Mockito.when(cm.getEstimatedLoad("F")).thenReturn(60.0);
		
		Mockito.when(cm.getCapacity("A")).thenReturn(10.0);
		Mockito.when(cm.getCapacity("B")).thenReturn(10.0);
		Mockito.when(cm.getCapacity("C")).thenReturn(10.0);
		Mockito.when(cm.getCapacity("D")).thenReturn(10.0);
		Mockito.when(cm.getCapacity("E")).thenReturn(10.0);
		Mockito.when(cm.getCapacity("F")).thenReturn(10.0);
		
		ComponentWindowedStats cwsLowSelect = Mockito.mock(ComponentWindowedStats.class);
		HashMap<Integer, Double> lowSelect = new HashMap<>();
		lowSelect.put(0, 0.3);
		Mockito.when(cwsLowSelect.getSelectivityRecords()).thenReturn(lowSelect);
		
		ComponentWindowedStats cwsMediumSelect = Mockito.mock(ComponentWindowedStats.class);
		HashMap<Integer, Double> mediumSelect = new HashMap<>();
		mediumSelect.put(0, 0.7);
		Mockito.when(cwsMediumSelect.getSelectivityRecords()).thenReturn(mediumSelect);
		
		ComponentWindowedStats cwsHighSelect = Mockito.mock(ComponentWindowedStats.class);
		HashMap<Integer, Double> highSelect = new HashMap<>();
		highSelect.put(0, 1.0);
		Mockito.when(cwsHighSelect.getSelectivityRecords()).thenReturn(highSelect);
		
		Mockito.when(cm.getStats("A")).thenReturn(cwsHighSelect);
		Mockito.when(cm.getStats("B")).thenReturn(cwsLowSelect);
		Mockito.when(cm.getStats("C")).thenReturn(cwsHighSelect);
		Mockito.when(cm.getStats("D")).thenReturn(cwsHighSelect);
		Mockito.when(cm.getStats("E")).thenReturn(cwsMediumSelect);
		Mockito.when(cm.getStats("F")).thenReturn(cwsHighSelect);
		
		ImpactMetric impactMetric = new ImpactMetric(cm, explorer);
		
		assertEquals(0.0, impactMetric.compute("A"), 0.0);
		assertEquals(70.0, impactMetric.compute("B"), 0.0);
		assertEquals(70.0, impactMetric.compute("C"), 0.0);
		assertEquals(90.0, impactMetric.compute("D"), 0.0);
		assertEquals(160.0, impactMetric.compute("E"), 0.0);
		assertEquals(84.0, impactMetric.compute("F"), 0.0);
	}

}
