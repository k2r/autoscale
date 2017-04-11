package storm.autoscale.scheduler.action;

public interface IActionTrigger extends Runnable{

	public void storeAction(String component, Integer currentDegree, Integer newDegree);
	
	public boolean isGracePeriod(String component);
	
	
}
