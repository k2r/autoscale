/**
 * 
 */
package storm.autoscale.scheduler.connector.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Roland
 *
 */
public class MySQLConnector implements IJDBCConnector {

	private static final String jdbcDriver = "com.mysql.jdbc.Driver";
	private String dbHost;
	private String dbName;
	private String dbUrl;
	private String dbUser;
	private String dbPassword;
	
	private Connection connection;
	
	public MySQLConnector(String dbHost, String dbName, String dbUser, String dbPassword) throws ClassNotFoundException, SQLException {
		this.dbHost = dbHost;
		this.dbName = dbName;
		this.dbUrl = "jdbc:mysql://"+ this.dbHost +"/" + this.dbName;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
		Class.forName(jdbcDriver);
		this.connection = DriverManager.getConnection(this.dbUrl, this.dbUser, this.dbPassword);
	}
	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.connector.database.IJDBCConnector#getConnection()
	 */
	@Override
	public Connection getConnection() {
		return this.connection;
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.connector.database.IJDBCConnector#getNewStatement()
	 */
	@Override
	public Statement getNewStatement() throws SQLException {
		Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		return statement;
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.connector.database.IJDBCConnector#executeUpdate(java.lang.String)
	 */
	@Override
	public int executeUpdate(String query) throws SQLException {
		Statement statement = this.getNewStatement();
		int result = statement.executeUpdate(query);
		return result;
	}

	/* (non-Javadoc)
	 * @see storm.autoscale.scheduler.connector.database.IJDBCConnector#executeQuery(java.lang.String)
	 */
	@Override
	public ResultSet executeQuery(String query) throws SQLException {
		Statement statement = this.getNewStatement();
		ResultSet result = statement.executeQuery(query);
		return result;
	}

}
