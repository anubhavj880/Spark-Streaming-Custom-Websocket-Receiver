package com.spark_reciver.SparkStreamingWebsocketReceiver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

public class WebsocketReceiver extends Receiver<String> {

	String uri;
	String schema;
	String storagePath;
	public WebsocketReceiver(String uri, String schema, String storagePath,StorageLevel storagelevel) {
		super(storagelevel);
		this.uri = uri;
		this.schema = schema;
		this.storagePath = storagePath;
	}

	@Override
	public void onStart() {

		new Thread(this::receive).start();
	}

	@Override
	public void onStop() {

	}

	public void receive() {
		WebSocketClient client = new WebSocketClient();
		CustomSocket socket = new CustomSocket();
		try {
			client.start();
			URI socketUri = new URI(uri);
			ClientUpgradeRequest request = new ClientUpgradeRequest();
			client.connect(socket, socketUri, request);
			socket.awaitClose(Integer.MAX_VALUE, TimeUnit.DAYS);
		} catch (Throwable t) {
			System.out.println(t.getMessage());
		} finally {
			try {
				client.stop();
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
		restart("Trying to connect again");
	}

	@WebSocket(maxTextMessageSize = 1024 * 1024)
	public class CustomSocket {
		 File file = new File(storagePath);
		 FileWriter fw = null;
		 BufferedWriter bw = null;
		private final CountDownLatch closeLatch;
		@SuppressWarnings("unused")
		private Session session;

		public CustomSocket() {
			this.closeLatch = new CountDownLatch(1);
		}

		public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
			return this.closeLatch.await(duration, unit);
		}

		@OnWebSocketClose
		public void onClose(int statusCode, String reason) {
//			System.out.printf("Connection closed: %d - %s%n", statusCode, reason);
			this.session = null;
			this.closeLatch.countDown(); // trigger latch
		}

		@OnWebSocketConnect
		public void onConnect(Session session) {
//			System.out.printf("Got connect: %s%n", session);
			this.session = session;
			try {
				Future<Void> fut = session.getRemote().sendStringByFuture(schema);
				fut.get(2, TimeUnit.SECONDS); // wait for send to complete.

			} catch (Throwable t) {
				System.out.println(t.getMessage());
			}
		}

		@OnWebSocketMessage
		public void onMessage(String msg) throws IOException {
			store(msg);
			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);
			bw.write(msg + "\n");
			bw.close();
			bw.close();
			fw.close();
		}
	}
}
