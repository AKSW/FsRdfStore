package org.aksw.jena_sparql_api.difs.main;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.apache.jena.vocabulary.RDF;

public class MainDifs {
	public static void main(String[] args) throws IOException {
		DatasetGraph dg = DifsFactory.newInstance().setPath(Paths.get("/tmp/testdb")).connect();
		Dataset d = DatasetFactory.wrap(dg);
		
		Txn.executeWrite(d, () -> {
			d.asDatasetGraph().add(RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.Property);
		});
	}
}
