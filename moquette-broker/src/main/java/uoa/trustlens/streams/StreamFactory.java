package uoa.trustlens.streams;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import eu.larkc.csparql.common.config.Config;
import eu.larkc.csparql.common.utils.CsparqlUtils;
import uoa.trutlens.csparql.IotStreamsEngine;

/**
 * @author mm
 *
 *        A singleton class for stream engine instance 
 */
public final class StreamFactory {

	private static StreamFactory single_instance = null;

	final IotStreamsEngine engine = IotStreamsEngine.forLiveData(m -> {
        //if non-compliance detected by the C-Sparql query -> save the resulting provenance trace to a file
		if (m.size() > 0) {
			// System.out.println ("Detected model size" + m.size());
			String fileName = "output_" + Long.toString(System.currentTimeMillis()) + ".ttl";
			FileWriter out;
			try {
				out = new FileWriter(fileName);

				try {
					m.write(out, "TTL");
				} finally {
					try {
						out.close();
					} catch (IOException closeException) {
						// ignore
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	});

	public StreamFactory() {

	}

	public IotStreamsEngine getEngine() {

		return engine;
	}

	// static method to create instance of Singleton class
	public static StreamFactory getInstance() {
		if (single_instance == null)
			single_instance = new StreamFactory();

		return single_instance;
	}
}
