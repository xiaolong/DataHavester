package xiaolong.baidu.libs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import flex.messaging.io.SerializationContext;
import flex.messaging.io.amf.ActionContext;
import flex.messaging.io.amf.ActionMessage;
import flex.messaging.io.amf.AmfMessageSerializer;
import flex.messaging.io.amf.MessageBody;
import flex.messaging.io.amf.MessageHeader;
import flex.messaging.io.amf.client.AMFConnection;
import flex.messaging.io.amf.client.exceptions.ClientStatusException;
import flex.messaging.io.amf.client.exceptions.ServerStatusException;

public class BaiduAMFConnection extends AMFConnection {

	@Override
	public Object call(String command, Object... arguments) throws ClientStatusException, ServerStatusException {
		// TODO: Support customizable batching of messages.
		ActionMessage requestMessage = new ActionMessage(getObjectEncoding());
		ActionContext actionContext = new ActionContext();
		SerializationContext serializationContext = new SerializationContext();

		if (amfHeaders != null){
			for (MessageHeader header : amfHeaders)
				requestMessage.addHeader(header);
		}

		MessageBody amfMessage = new MessageBody(command, "/1M", arguments);
		requestMessage.addBody(amfMessage);

		// Setup for AMF message serializer
		actionContext.setRequestMessage(requestMessage);
		ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
		AmfMessageSerializer amfMessageSerializer = new AmfMessageSerializer();
		amfMessageSerializer.initialize(serializationContext, outBuffer, null);

		try {
			amfMessageSerializer.writeMessage(requestMessage);
			Object result = send(outBuffer);
			return result;
		} catch (Exception e) {
			if (e instanceof ClientStatusException)
				throw (ClientStatusException) e;
			else if (e instanceof ServerStatusException)
				throw (ServerStatusException) e;
			// Otherwise, wrap into a ClientStatusException.
			ClientStatusException exception = new ClientStatusException(e, ClientStatusException.AMF_CALL_FAILED_CODE);
			throw exception;
		} finally {
			try {
				outBuffer.close();
			} catch (IOException ignore){}
		}
	}

	/**
﻿   * 
﻿   * @param args
﻿   * @throws IOException
﻿   * @throws ClientStatusException
﻿   * @throws ServerStatusException
﻿   */
	public static void main(String[] args) throws IOException, ClientStatusException, ServerStatusException {
		AMFConnection amfConnection = new BaiduAMFConnection();
		amfConnection.setObjectEncoding(3);

		String url = "http://index.baidu.com/gateway.php";

		amfConnection.addHttpRequestHeader("Content-type", "application/x-amf");
		amfConnection.addHttpRequestHeader("Referer", "http://index.baidu.com/fla/TrendAnalyserfc9e1047.swf");
		amfConnection.connect(url);
		Object body = amfConnection.call("DataAccessor.getIndexes", "iphone", "0", "", "cd01976518550a2541", "ca0f8b1973971802f7c660eb21aa0077");

		Object[] arr = (Object[]) body;
		Map map = (Map) arr[0];
		System.out.println(map.get("userIndexes"));
		amfConnection.close();
	}

}