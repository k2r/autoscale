/**
 * 
 */
package storm.autoscale.scheduler.config;

/**
 * @author Roland
 *
 */
public enum ParameterNames {
	
	PARAM("parameters"),
	NIMBHOST("nimbus_host"),
	NIMBPORT("nimbus_port"),
	MONITFREQ("monitoring_frequency"),
	WINSIZE("window_size"),
	LOWACT("low_activity_threshold"),
	HIGHACT("high_activity_threshold"),
	STABCOEFF("stabilization_coeff"),
	SLOPE("slope_threshold"),
	DBHOST("db_host"),
	DBNAME("db_name"),
	DBUSER("db_user"),
	DBPWD("db_password");
	
	
	private String name;
	
	private ParameterNames(String name){
		this.name = name;
	}

	public String toString(){
		return this.name;
	}
}
