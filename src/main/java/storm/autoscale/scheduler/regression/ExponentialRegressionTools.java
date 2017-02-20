/**
 * 
 */
package storm.autoscale.scheduler.regression;

import java.util.HashMap;

/**
 * @author Roland
 *
 */
public class ExponentialRegressionTools {

	public static <T extends Number, U extends Number> HashMap<Double, Double> linearizeCoordinates(HashMap<T, U> coordinates){
		HashMap<Double, Double> result = new HashMap<>();
		for(T xCoordinate : coordinates.keySet()){
			Double yCoordinate = coordinates.get(xCoordinate).doubleValue();
			if(yCoordinate < 1){
				yCoordinate = 1.0;//we degrade slightly the estimation but it allows to compute a model
			}
			result.put(xCoordinate.doubleValue(), Math.log(yCoordinate));
		}
		return result;
	}
	
	public static <T extends Number, U extends Number> Double regressionCoeff(HashMap<T, U> coordinates){
		HashMap<Double, Double> linearized = ExponentialRegressionTools.linearizeCoordinates(coordinates);
		return LinearRegressionTools.regressionCoeff(linearized);
	}
	
	public static <T extends Number, U extends Number> Double regressionOffset(HashMap<T, U> coordinates){
		HashMap<Double, Double> linearized = ExponentialRegressionTools.linearizeCoordinates(coordinates);
		return LinearRegressionTools.regressionOffset(linearized);
	}
	
	public static <T extends Number, U extends Number> Double correlationCoeff(HashMap<T, U> coordinates){
		HashMap<Double, Double> linearized = ExponentialRegressionTools.linearizeCoordinates(coordinates);
		return LinearRegressionTools.correlationCoeff(linearized);
	}
	
	public static <T extends Number, U extends Number> Double estimateYCoordinate(T xCoordinate, HashMap<T, U> coordinates){
		Double regressionCoeff = ExponentialRegressionTools.regressionCoeff(coordinates);
		Double regressionOffset = ExponentialRegressionTools.regressionOffset(coordinates);
		return (regressionCoeff * Math.exp(regressionOffset * xCoordinate.doubleValue()));
	}
}