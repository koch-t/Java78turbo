package nl.skywave.java78turbo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

//
// Reading from multiple sockets in Java
// This version uses ZMQ.Poller
//
// Nicola Peduzzi <thenikso@gmail.com>
//
public class ZeroMQclient {

	public static void main(String[] args) throws IOException {
		ZMQ.Context context = ZMQ.context(1);

		ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
		subscriber.connect("tcp://localhost:7817");
		subscriber.subscribe("/GOVI/KV8".getBytes());

		// Process messages from both sockets
		while (true) {
			ZMsg msg = ZMsg.recvMsg(subscriber);
			try{
				Iterator<ZFrame> msgs = msg.iterator();
				msgs.next();
				while (msgs.hasNext()){
					byte[] data = msgs.next().getData();
					InputStream gzipped = new ByteArrayInputStream(data);
					InputStream in = new GZIPInputStream(gzipped);
					StringBuffer out = new StringBuffer();
					byte[] b = new byte[4096];
					for (int n; (n = in.read(b)) != -1;) {
						out.append(new String(b, 0, n));
					}
					String s = out.toString();
					CTX c = new CTX(s);
					System.out.println(c.rows.get(0).get("LinePlanningNumber"));
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}