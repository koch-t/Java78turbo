package nl.skywave.java78turbo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.zip.GZIPInputStream;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

//
// Java example on how to receive messages from OpenOV's KV78turbo ZeroMQ pubsub and parse the receiving CTX
//
//
public class ZeroMQclient {

	public static void main(String[] args) throws IOException {
		ZMQ.Context context = ZMQ.context(1);

		ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
		subscriber.connect("tcp://localhost:7817");
		subscriber.subscribe("/GOVI/KV8".getBytes());

		while (true) {
			ZMsg msg = ZMsg.recvMsg(subscriber);
			try{
				Iterator<ZFrame> msgs = msg.iterator();
				msgs.next();
				ArrayList<Byte> receivedMsgs = new ArrayList<Byte>();
				while (msgs.hasNext()){
					for (byte b : msgs.next().getData()){
						receivedMsgs.add(b);
					}
				}
				byte[] fullMsg = new byte[receivedMsgs.size()];
				for (int i = 0; i < fullMsg.length; i++){
					fullMsg[i] = receivedMsgs.get(i);
				}
				InputStream gzipped = new ByteArrayInputStream(fullMsg);
				InputStream in = new GZIPInputStream(gzipped);
				StringBuffer out = new StringBuffer();
				byte[] b = new byte[4096];
				for (int n; (n = in.read(b)) != -1;) {
					out.append(new String(b, 0, n));
				}
				String s = out.toString();
				CTX c = new CTX(s);
				for (int i = 0; i < c.rows.size(); i++){
					HashMap<String,String> row = c.rows.get(i);
					System.out.println(row.get("LinePlanningNumber") + " " + row.get("TripStopStatus") + " " + row.get("ExpectedDepartureTime") + " " +row.get("TimingPointCode"));
				}

			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}