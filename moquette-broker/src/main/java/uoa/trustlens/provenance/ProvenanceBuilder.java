package uoa.trustlens.provenance;

import io.moquette.broker.subscriptions.Topic;
import uoa.trustlens.streams.StreamFactory;

public class ProvenanceBuilder implements Runnable {

	PublishTraceTemplate template;
	Topic topic;
	String clientId, publisherClientId;
	long publishStarted;

	public ProvenanceBuilder(Topic topic, String clientId, String publisherClientId, long publishStarted) {
		this.clientId = clientId;
		this.publisherClientId = publisherClientId;
		this.topic = topic;
		this.publishStarted = publishStarted;

	};

	public PublishTraceTemplate getPublishTraceTemplate() {
		return template;
	}

	@Override
	public void run() {
		template = new PublishTraceTemplate(topic, publishStarted);
		template.addAffectedClient(clientId, publisherClientId);
		StreamFactory stream = StreamFactory.getInstance();
		stream.getEngine().accept(template.getModel());

	}

}
