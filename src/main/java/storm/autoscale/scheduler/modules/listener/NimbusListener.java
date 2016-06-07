/**
 * 
 */
package storm.autoscale.scheduler.modules.listener;

import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;

import backtype.storm.generated.Nimbus;

/**
 * @author Roland
 *
 */
public class NimbusListener {

	private TSocket tsocket;
	private TFramedTransport tTransport;
	private TBinaryProtocol tBinaryProtocol;
	private Nimbus.Client client;
	private static NimbusListener instance = null;
	
	/**
	 * 
	 */
	private NimbusListener(String nimbusHost, Integer nimbusPort) {
		this.tsocket = new TSocket(nimbusHost, nimbusPort);
		this.tTransport = new TFramedTransport(tsocket);
		this.tBinaryProtocol = new TBinaryProtocol(tTransport);
		this.client = new Nimbus.Client(tBinaryProtocol);
	}
	
	public static NimbusListener getInstance(String nimbusHost, Integer nimbusPort){
		if(instance == null){
			instance = new NimbusListener(nimbusHost, nimbusPort);
		}
		return instance;
	}

	/**
	 * @return the tTransport
	 */
	public TFramedTransport gettTransport() {
		return tTransport;
	}

	/**
	 * @param tTransport the tTransport to set
	 */
	public void settTransport(TFramedTransport tTransport) {
		this.tTransport = tTransport;
	}

	/**
	 * @return the client
	 */
	public Nimbus.Client getClient() {
		return client;
	}

	/**
	 * @param client the client to set
	 */
	public void setClient(Nimbus.Client client) {
		this.client = client;
	}
}