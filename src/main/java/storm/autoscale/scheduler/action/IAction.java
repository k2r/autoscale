/**
 * 
 */
package storm.autoscale.scheduler.action;

/**
 * @author Roland
 *
 */
public interface IAction extends Runnable{
	
	public void storeAction(String component, Integer currentDegree, Integer newDegree);
	
	public boolean isGracePeriod(String component);
}
