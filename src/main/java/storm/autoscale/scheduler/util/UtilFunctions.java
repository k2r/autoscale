/**
 * 
 */
package storm.autoscale.scheduler.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;

/**
 * @author Roland
 *
 */
public class UtilFunctions {

	public static ArrayList<ArrayList<Integer>> getBuckets(ArrayList<Integer> values, Integer nbBuckets){
		ArrayList<ArrayList<Integer>> result = new ArrayList<>();
		int nbTasks = values.size();
		int quotient = nbTasks / nbBuckets;
		int remainder = nbTasks - (nbBuckets * quotient);
		if(remainder == 0){
			for(int i = 0; i < nbTasks; i += quotient){
				ArrayList<Integer> delimiters = new ArrayList<>();
				int start = values.get(i);
				int end = values.get(i + quotient - 1);
				delimiters.add(start);
				delimiters.add(end);
				result.add(delimiters);
			}
		}else{
			for(int i = 0; i < nbTasks - (quotient + remainder); i += quotient){
				ArrayList<Integer> delimiters = new ArrayList<>();
				int start = values.get(i);
				int end = values.get(i + quotient - 1);
				delimiters.add(start);
				delimiters.add(end);
				result.add(delimiters);
			}
			ArrayList<Integer> delimiters = new ArrayList<>();
			int start = values.get(nbTasks - (quotient + remainder));
			int end = values.get(nbTasks - 1);
			delimiters.add(start);
			delimiters.add(end);
			result.add(delimiters);
		}
		return result;
	}
	
	public static <T, U extends Number> T getMaxCategory(HashMap<T, U> values){
		T maxCategory = null;
		Double maxValue = 0.0;
		for(T category : values.keySet()){
			U value = values.get(category);
			if(Math.abs(value.doubleValue()) > Math.abs(maxValue.doubleValue())){
				maxCategory = category;
				maxValue = value.doubleValue();
			}
		}
		return maxCategory;
	}
	
	public static class DecreasingIntOrder implements Comparator<Integer>{

		@Override
		public int compare(Integer o1, Integer o2) {
			if(o1.intValue() < o2.intValue()){
				return 1;
			}
			return -1;
		}
		
	}
	
	public static class DecreasingDoubleOrder implements Comparator<Double>{

		@Override
		public int compare(Double o1, Double o2) {
			if(o1.doubleValue() < o2.doubleValue()){
				return 1;
			}
			return -1;
		}
		
	}
	
	public static <T extends Number, U extends Number> ArrayList<U> getValues(HashMap<T, U> map){
		ArrayList<U> values = new ArrayList<>();
		Set<T> keys = map.keySet();
		for(T key : keys){
			U value = map.get(key);
			values.add(value);
		}
		return values;
	}
	
	public static <T extends Number> Double getAvgValue(ArrayList<T> values){
		Double avgValue = 0.0;
		int nbValues = values.size();
		for(int i = 0; i < nbValues; i++){
			avgValue += values.get(i).doubleValue();
		}
		return avgValue / nbValues;
	}
	
	public static <T extends Number> T getMaxValue(ArrayList<T> values){
		T max = null;
		if(!values.isEmpty()){
			max = values.get(0);
			int nbValues = values.size();
			for(int i = 1; i < nbValues; i++){
				if(values.get(i).doubleValue() > max.doubleValue()){
					max = values.get(i);
				}
			}
		}
		return max;
	}
	
	public static <T extends Number> T getMinValue(ArrayList<T> values){
		T min = null;
		if(!values.isEmpty()){
			min = values.get(0);
			int nbValues = values.size();
			for(int i = 1; i < nbValues; i++){
				if(values.get(i).doubleValue() < min.doubleValue()){
					min = values.get(i);
				}
			}
		}
		return min;
	}
	
	public static <T extends Number> Double getStdDerivation(ArrayList<T> values){
		Double stdDerivation = 0.0;
		if(!values.isEmpty()){
			Double sqrSum = 0.0;
			Double avg = UtilFunctions.getAvgValue(values);
			for(T value : values){
				sqrSum += Math.pow((value.doubleValue() - avg), 2);
			}
			Double variance = sqrSum / values.size();
			stdDerivation = Math.sqrt(variance);
		}
		return stdDerivation;
	}
	
	public static void delete(File f) throws IOException {
		if (f.isDirectory()) {
			for (File c : f.listFiles())
				delete(c);
		}
		if (!f.delete())
			throw new FileNotFoundException("Failed to delete file: " + f);
	}
}