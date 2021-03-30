package org.aksw.difs.example.main;

import java.io.IOException;
import java.nio.file.Paths;

import org.aksw.jena_sparql_api.difs.main.DifsFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.apache.jena.vocabulary.RDF;

public class MainPlayground {
	public static void main(String[] args) throws IOException {
		DatasetGraph dg = DifsFactory.newInstance()
				.setPath(Paths.get("/tmp/testdb"))
				.addIndex(RDF.Nodes.type, "type")
				.connect();
		Dataset d = DatasetFactory.wrap(dg);

		Txn.executeWrite(d, () -> {
			RDFDataMgr.read(dg, "/home/raven/Projects/Eclipse/cord19-rdf/rdfize/data-1000.trig");
		});

		
//		Txn.executeWrite(d, () -> {
//			d.asDatasetGraph().delete(RDF.Nodes.first, RDF.Nodes.first, RDF.Nodes.type, RDF.Nodes.Property);
//		});
//
//		Txn.executeWrite(d, () -> {
//			d.asDatasetGraph().add(RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.Property);
//			d.asDatasetGraph().add(RDF.Nodes.first, RDF.Nodes.first, RDF.Nodes.type, RDF.Nodes.Property);
//		});
	
		
//		RDFDataMgr.write(System.out, d, RDFFormat.TRIG_PRETTY);
		

		d.asDatasetGraph().find(Node.ANY, Node.ANY, RDF.Nodes.type, RDF.Nodes.Property)
			.forEachRemaining(x -> System.out.println("Got result: " + x));
		
	}
}
