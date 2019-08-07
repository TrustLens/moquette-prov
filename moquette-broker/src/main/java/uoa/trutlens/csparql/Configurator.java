package uoa.trutlens.csparql;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import eu.larkc.csparql.common.utils.CsparqlUtils;
import eu.larkc.csparql.core.engine.CsparqlEngine;
import uoa.trustlens.util.IotStreamsException;
import uoa.trustlens.util.Logging;


/**
 * 
 * @author nhc
 *
 * A Configurator loads configuration from a directory
 * and configures a given C-SPARQL Engine accordingly.
 */
final class Configurator {
    /** Charset used for decoding all meat probe files */
    private static final Charset ISO88591 = Charset.forName("ISO-8859-1");

    private static final String QUERY_FILE = "csparql-query.rq";
    private static final String ONTOLOGY_FILE = "init.ttl";
    
    /** Root of all configuration files */
    private static final Path CONFIG_ROOT = Paths.get("config/iotstreams/");
    
    /** The engine to configure */
    private final CsparqlEngine engine;

    /** Observer for each query */
    private Map<String, IotStreamsFormatter> observers = new HashMap<>();

    /** The persistent model to add inferred triples to */
    private final Consumer<Model> inferredTripleConsumer;
    
    /**
     * Registers the engine to configure
     * @param engine The engine to configure
     * @param inferredTripleConsumer The object to pass inferred triples to
     */
    public Configurator(final CsparqlEngine engine, final Consumer<Model> inferredTripleConsumer) {
        this.engine = engine;
        this.inferredTripleConsumer = inferredTripleConsumer;
        try {
           Iterator<Path> it =  Files.walk(CONFIG_ROOT)
                .filter(Files::isRegularFile)
                .iterator();
           //horrible hack just change in the future if there is time
           //the problem is that on mac the files are read from bottom to top and on linux from top to bottom -> c-sparql complains if queruy is registered before static knowledge is loaded
           while (it.hasNext()) {
        	   Path path = it.next();
        	   System.out.println("hello"+path);
        	   if (path.toString().contains("csparql-quer")) {
        		   addFile(it.next()); 
        		   addFile(path); 
        	   }
        	   else {
        		   addFile(path); 
        		   addFile(it.next()); 
        	   }
           }
        } catch (final IOException e) {
            throw IotStreamsException.configurationError(e);
        }
    }
    
    /**
     * Reads file content and interprets the file path in order to add this content
     * to the right place.
     * @param file A file in the configuration dir.
     */
    private void addFile(final Path file) {
        Logging.info(String.format("Loading %s...", file.toString()));
        
        final String content = read(file);
        final Path rel = CONFIG_ROOT.relativize(file);
        System.out.println ("conditions: "+rel.getFileName() + " "+ rel.getNameCount());
        if (rel.getNameCount() == 2 && rel.getFileName().toString().equals(QUERY_FILE)) {
            //<CONFIG_ROOT>/<name>/csparql-query.rq: A C-SPARQL query
            this.registerQuery(rel.getName(0), content);
        } else if (rel.getNameCount() == 2 && rel.getFileName().toString().equals(ONTOLOGY_FILE)) {
            //<CONFIG_ROOT>/<name>/init.ttl: A TTL ontology for initializing our Jena model
            //put back if we want static knowledge the old way
        	//formatter(rel.getName(0)).setOntology(content);
        	System.out.println ("path: "+file.toString());
        	 try {
        		 
        		
				this.engine.putStaticNamedModel("https://trustlens.org/trustedAgents", CsparqlUtils.serializeJenaModel(ModelFactory.createDefaultModel().read(file.toString())));
			} catch (Exception e) {
				System.out.println("cant find teh static knowledge ontology file");
				e.printStackTrace();
			}

        }  else {
            throw IotStreamsException.configurationError(String.format("Unexpected file %s at depth %d, filename=%s", rel.toString(), rel.getNameCount(), rel.getFileName().toString()));
        }
    }
    
    /**
     * Registers a new C-SPARQL query.
     * @param name Derived from the file path
     * @param content The text of the C-SPARQL query
     */
    private void registerQuery(final Path name, final String content) {
        try {
        	System.out.println("Registering query " + content);
            this.engine.registerQuery(
                    content, 
                    false)
                .addObserver(formatter(name));
        } catch (final ParseException e) {
            throw IotStreamsException.configurationError(e);
        }
    }
    
    private IotStreamsFormatter formatter(final Path nameAsPath) {
        final String name = nameAsPath.toString();
        if (!this.observers.containsKey(name)){
            this.observers.put(name, new IotStreamsFormatter(name, this.inferredTripleConsumer));
        }
        return this.observers.get(name.toString());
    }
    /**
     * Utility for reading a file entirely, as ISO88591
     * @param file Any regular file
     * @return The file's content
     */
    private static String read(final Path file) {
        try {
            return String.join("\n", Files.readAllLines(file, ISO88591));
        } catch (final IOException e) {
            throw IotStreamsException.configurationError(e);
        }
    }
}