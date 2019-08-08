package uoa.trutlens.csparql;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Observable;
import java.util.Optional;
import java.util.function.Consumer;



import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.util.NodeFactoryExtra;
import com.hp.hpl.jena.update.UpdateAction;
import com.hp.hpl.jena.vocabulary.RDF;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.core.ResultFormatter;
import uoa.trustlens.util.IotStreamsException;
import uoa.trustlens.util.Logging;

/**
 * 
 * @author nhc
 *
 * An IotStreamsFormatter listens to one C-SPARQL query.
 * When the query yields a result set (i.e. triples for one window),
 * the IotStreamsFormatter executes the registered SPARQL updates
 * and handles results.
 */
class IotStreamsFormatter extends ResultFormatter {
    /** Name picked up from directory - used for logging */
    private final String queryName;
    

    /** Model containing the ontology/static knowledge */
    private Optional<OntModel> staticKnowledge = Optional.empty();

    /** Model storing the received triples along with the ontology */
    private Optional<Model> m = Optional.empty();


    private int resultCounter = 1;

    /** Final resting place for inferred triples */
    private final Consumer<Model> inferredTripleConsumer;

    /**
     * Registers the query name and prepares for configuration by
     * addSparql() and setOntology()
     * @param queryName Name picked up from directory - used for logging
     * @param inferredTripleConsumer All inferred triples will be passed to this object
     */
    IotStreamsFormatter(final String queryName, final Consumer<Model> inferredTripleConsumer) {
        this.queryName = queryName;
        this.inferredTripleConsumer = inferredTripleConsumer;

    }
    
    /**
     * Called when C-Sparql yields a set of results from the assoaciated C-SPARQL query.
     * Adds all triples to the internal model, then runs infer().
     */
    @Override
    public synchronized void update(final Observable ignored, final Object rdfTableUntyped) {
    	
        final RDFTable rdfTable = (RDFTable) rdfTableUntyped;
        this.m = Optional.of(ModelFactory.createDefaultModel());
       
        rdfTable.stream()
            .map(this::convert)
            .forEach(s -> this.m.get().add(s));
      
        this.inferredTripleConsumer.accept(this.m.get());
      //  if (resultCounter%5==0) {
        System.out.println("C-SPARQL result : " + resultCounter++);
        System.out.println("C-SPARQL result : " + System.currentTimeMillis());
       // }
        //else {
        	//resultCounter++;
        //}
       
    }
    
    /**
     * Converts a in an RdfTuple to a Jena Statement.
     * @param t must have URIs as element 0 and 1, and a URI or a Literal as element 2
     * @return The Statement representing elements 0,1,2 of t
     */
    private Statement convert(final RDFTuple t) {
        RDFNode o;
       
        final String[] oSplit = t.get(2).split("\\^\\^"); //Split if this is a typed literal
        if (oSplit.length == 2) {
            o = this.m.get().asRDFNode(NodeFactoryExtra.createLiteralNode(
                    oSplit[0].substring(1, oSplit[0].length() - 1), 
                    null, 
                    oSplit[1]));
        } else {
            o = this.m.get().createResource(t.get(2));            
        }
        try { //Compose and return the Statement
            return this.m.get().createStatement(
                    this.m.get().createResource(t.get(0)), 
                    this.m.get().createProperty(t.get(1)),
                    o);
        } catch (final Exception e) {
            throw IotStreamsException.internalError(String.format("Problem converting %s", t.get(2)));
        }
    }

   
}