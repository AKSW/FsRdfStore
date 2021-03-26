package org.aksw.jena_sparql_api.difs.main;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.apache.jena.vocabulary.RDF;

public class MainPlayground {
	public static void main(String[] args) throws IOException {
		DatasetGraph dg = DifsFactory.newInstance().setPath(Paths.get("/tmp/testdb")).connect();
		Dataset d = DatasetFactory.wrap(dg);

		Txn.executeWrite(d, () -> {
			d.asDatasetGraph().delete(RDF.Nodes.first, RDF.Nodes.first, RDF.Nodes.type, RDF.Nodes.Property);
		});

		Txn.executeWrite(d, () -> {
			d.asDatasetGraph().add(RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.Property);
			d.asDatasetGraph().add(RDF.Nodes.first, RDF.Nodes.first, RDF.Nodes.type, RDF.Nodes.Property);
		});
	
		
		RDFDataMgr.write(System.out, d, RDFFormat.TRIG_PRETTY);
	}
}
