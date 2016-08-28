/**
 * 
 */
package storm.autoscale.scheduler.util;

import java.util.HashMap;

/**
 * @author Roland
 *
 */
public class Regression {
	
	public static <T extends Number, U extends Number> Integer nbPoints(HashMap<T, U> coordinates){
		return coordinates.size();
	}
	
	public static <T extends Number, U extends Number> Double sumXCoordinate(HashMap<T, U> coordinates){
		Double sum = 0.0;
		for(T xCoordinate : coordinates.keySet()){
			sum += xCoordinate.doubleValue();
		}	
		return sum;
	}
	
	public static <T extends Number, U extends Number> Double sumYCoordinate(HashMap<T, U> coordinates){
		Double sum = 0.0;
		for(U yCoordinate : coordinates.values()){
			sum += yCoordinate.doubleValue();
		}	
		return sum;
	}
	
	public static <T extends Number, U extends Number> Double sumProdXYCoordinates(HashMap<T, U> coordinates){
		Double sum = 0.0;
		for(T xCoordinate : coordinates.keySet()){
			U yCoordinate = coordinates.get(xCoordinate);
			Double product = xCoordinate.doubleValue() * yCoordinate.doubleValue();
			sum += product;
		}
		return sum;
	}
	
	public static <T extends Number, U extends Number> Double sumSqXCoordinates(HashMap<T, U> coordinates){
		Double sum = 0.0;
		for(T xCoordinate : coordinates.keySet()){
			Double product = xCoordinate.doubleValue() * xCoordinate.doubleValue();
			sum += product;
		}
		return sum;
	}
	
	public static <T extends Number, U extends Number> Double avgXCoordinate(HashMap<T, U> coordinates){
		return Regression.sumXCoordinate(coordinates) / Regression.nbPoints(coordinates);
	}
	
	public static <T extends Number, U extends Number> Double avgYCoordinate(HashMap<T, U> coordinates){
		return Regression.sumYCoordinate(coordinates) / Regression.nbPoints(coordinates);
	}
	
	public static <T extends Number, U extends Number> Double linearRegressionCoeff(HashMap<T, U> coordinates){
		Double dividend = (Regression.nbPoints(coordinates) * Regression.sumProdXYCoordinates(coordinates)) 
				- (Regression.sumXCoordinate(coordinates) * Regression.sumYCoordinate(coordinates));
		Double divisor = (Regression.nbPoints(coordinates) * Regression.sumSqXCoordinates(coordinates))
				- (Math.pow(Regression.sumXCoordinate(coordinates), 2));
		return dividend / divisor;
	}
	
	public static <T extends Number, U extends Number> Double linearRegressionOffset(HashMap<T, U> coordinates){
		return Regression.avgYCoordinate(coordinates) - (Regression.linearRegressionCoeff(coordinates) * Regression.avgXCoordinate(coordinates));
	}
}