package uoa.trustlens.provenance;

import java.util.UUID;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

import io.moquette.broker.subscriptions.Topic;

/**
 * @author mm Provenance trace template Genereates 27 triples describing
 *         provenance trace of mqtt Publish step
 * 
 * 
 * 
 */

public class PublishTraceTemplate {

	String executionTraceBundleID = UUID.randomUUID().toString();
	String publishActivityID = UUID.randomUUID().toString();
	String recipoientCollectionID = UUID.randomUUID().toString();
	String topicEntityID = UUID.randomUUID().toString();
	String actionResultID = UUID.randomUUID().toString();

	// String publishTrace ="";
	Model m = ModelFactory.createDefaultModel();

	Resource traceBundle;
	Resource publishActivity;
	Resource collection;
	Resource actionresult;

	Property used = m.createProperty("http://www.w3.org/ns/prov#used");
	Property value = m.createProperty("http://www.w3.org/ns/prov#value");
	Property wgb = m.createProperty("http://www.w3.org/ns/prov#wasGeneratedBy");
	Property isElementOfTrace = m.createProperty("https://w3id.org/ep-plan#isElementOfTrace");
	Property corrToStep = m.createProperty("https://w3id.org/ep-plan#correspondsToStep");
	Property corrToVar = m.createProperty("https://w3id.org/ep-plan#correspondsToVariable");
	Property wdf = m.createProperty("http://www.w3.org/ns/prov#wasDerivedFrom");
	Property hadMember = m.createProperty("http://www.w3.org/ns/prov#hadMember");
	Property started = m.createProperty("http://www.w3.org/ns/prov#startedAtTime");
	Property alternateOf = m.createProperty("http://www.w3.org/ns/prov#alternateOf");

	Resource entityType = m.createResource("https://w3id.org/ep-plan#Entity");
	Resource agentType = m.createResource("https://w3id.org/ep-plan#Agent");
	Resource entityCollectionType = m.createResource("https://w3id.org/ep-plan#EntityCollection");

	public PublishTraceTemplate(Topic topic, long publishStarted) {

		Resource traceBundleType = m.createResource("https://w3id.org/ep-plan#ExecutionTraceBundle");
		traceBundle = m.createResource("http://trustlens.org/mqtt/publish/trace/bundle/" + executionTraceBundleID);
		traceBundle.addProperty(RDF.type, traceBundleType);
		traceBundle.addProperty(wdf, m.createResource("http://example.com/mqtt-plan#Publish"));

		// topic
		Resource topicEntity = m.createResource("http://trustlens.org/mqtt/publish/trace/bundle/"
				+ executionTraceBundleID + "/entity/" + topicEntityID);
		topicEntity.addProperty(RDF.type, entityType);
		Literal topicLit = m.createTypedLiteral(topic.toString());
		topicEntity.addProperty(value, topicLit);
		topicEntity.addProperty(isElementOfTrace, traceBundle);
		topicEntity.addProperty(corrToVar, m.createResource("http://example.com/mqtt-plan#Topic"));

		Resource publishActivitytype = m.createResource("https://w3id.org/ep-plan#Activity");
		publishActivity = m.createResource("http://trustlens.org/mqtt/publish/trace/bundle/" + executionTraceBundleID
				+ "/activity/" + publishActivityID);
		publishActivity.addProperty(RDF.type, publishActivitytype);
		publishActivity.addProperty(used, topicEntity);
		publishActivity.addProperty(isElementOfTrace, traceBundle);
		publishActivity.addProperty(corrToStep, m.createResource("http://example.com/mqtt-plan#PublishStep"));
		publishActivity.addProperty(started, m.createTypedLiteral(publishStarted));

		actionresult = m.createResource("http://trustlens.org/mqtt/publish/trace/bundle/" + executionTraceBundleID
				+ "/action_result/" + actionResultID);
		actionresult.addProperty(RDF.type, entityCollectionType);
		actionresult.addProperty(wgb, publishActivity);
		actionresult.addProperty(corrToVar, m.createResource("http://example.com/mqtt-plan#PublishResult"));
		actionresult.addProperty(isElementOfTrace, traceBundle);
	}

	public void addAffectedClient(String clientID, String publisherClientId) {
		String entityID = UUID.randomUUID().toString();
		String senderID = UUID.randomUUID().toString();

		Resource agentEntity = m.createResource(
				"http://trustlens.org/mqtt/publish/trace/bundle/" + executionTraceBundleID + "/entity/" + entityID);
		agentEntity.addProperty(RDF.type, entityType);
		agentEntity.addProperty(RDF.type, agentType);
		agentEntity.addProperty(alternateOf, m.createResource("https://trustlens.org#" + clientID));
		agentEntity.addProperty(isElementOfTrace, traceBundle);
		agentEntity.addProperty(corrToVar, m.createResource("http://example.com/mqtt-plan#AffectedAgent"));
		actionresult.addProperty(hadMember, agentEntity);

		Resource senderEntity = m.createResource(
				"http://trustlens.org/mqtt/publish/trace/bundle/" + executionTraceBundleID + "/entity/" + senderID);
		senderEntity.addProperty(RDF.type, entityType);
		senderEntity.addProperty(RDF.type, agentType);
		senderEntity.addProperty(alternateOf, m.createResource("https://trustlens.org#" + publisherClientId));
		senderEntity.addProperty(isElementOfTrace, traceBundle);
		senderEntity.addProperty(corrToVar, m.createResource("http://example.com/mqtt-plan#Sender"));
		publishActivity.addProperty(used, senderEntity);
	}

	public Model getModel() {
		// System.out.println(m.size());
		return m;
	}

}
