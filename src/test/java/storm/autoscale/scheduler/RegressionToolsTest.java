/**
 * 
 */
package storm.autoscale.scheduler;

import java.util.HashMap;

import junit.framework.TestCase;
import storm.autoscale.scheduler.util.RegressionTools;

/**
 * @author Roland
 *
 */
public class RegressionToolsTest extends TestCase {

	private static HashMap<Integer, Long> coordinates;
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		coordinates = new HashMap<>();
		coordinates.put(1, 0L);
		coordinates.put(2, 100L);
		coordinates.put(3, 120L);
		coordinates.put(4, 40L);
		coordinates.put(5, 75L);
		coordinates.put(6, 30L);
		coordinates.put(7, 90L);
		coordinates.put(8, 10L);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.util.RegressionTools#nbPoints(java.util.HashMap)}.
	 */
	public void testNbPoints() {
		assertEquals(8.0, RegressionTools.nbPoints(coordinates), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.util.RegressionTools#sumXCoordinate(java.util.HashMap)}.
	 */
	public void testSumXCoordinate() {
		assertEquals(36.0, RegressionTools.sumXCoordinate(coordinates), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.util.RegressionTools#sumYCoordinate(java.util.HashMap)}.
	 */
	public void testSumYCoordinate() {
		assertEquals(465.0, RegressionTools.sumYCoordinate(coordinates), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.util.RegressionTools#sumProdXYCoordinates(java.util.HashMap)}.
	 */
	public void testSumProdXYCoordinates() {
		assertEquals(1985.0, RegressionTools.sumProdXYCoordinates(coordinates), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.util.RegressionTools#sumSqXCoordinates(java.util.HashMap)}.
	 */
	public void testSumSqXCoordinates() {
		assertEquals(204.0, RegressionTools.sumSqXCoordinates(coordinates), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.util.RegressionTools#avgXCoordinate(java.util.HashMap)}.
	 */
	public void testAvgXCoordinate() {
		assertEquals(4.5, RegressionTools.avgXCoordinate(coordinates), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.util.RegressionTools#avgYCoordinate(java.util.HashMap)}.
	 */
	public void testAvgYCoordinate() {
		assertEquals(58.125, RegressionTools.avgYCoordinate(coordinates), 0);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.util.RegressionTools#linearRegressionCoeff(java.util.HashMap)}.
	 */
	public void testLinearRegressionCoeff() {
		assertEquals(-2.5595, RegressionTools.linearRegressionCoeff(coordinates), 0.001);
	}

	/**
	 * Test method for {@link storm.autoscale.scheduler.util.RegressionTools#linearRegressionOffset(java.util.HashMap)}.
	 */
	public void testLinearRegressionOffset() {
		assertEquals(69.6428, RegressionTools.linearRegressionOffset(coordinates), 0.001);
	}

}
