package org.aksw.difs.example.main;

import java.io.IOException;
import java.nio.file.Paths;

import org.aksw.common.io.util.symlink.SymbolicLinkStrategies;
import org.aksw.difs.builder.DifsFactory;
import org.aksw.difs.engine.QueryExecutionFactoryQuadForm;
import org.aksw.difs.index.impl.RdfIndexerFactoryLexicalForm;
import org.aksw.difs.index.impl.RdfTermIndexerFactoryIriToFolder;
import org.aksw.difs.system.domain.StoreDefinition;
import org.aksw.jena_sparql_api.dataset.file.DatasetGraphIndexerFromFileSystem;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

public class MainPlayground {
		
	public static void main3(String[] args) throws IOException {
		DatasetGraph dg = DifsFactory.newInstance()
				.setSymbolicLinkStrategy(SymbolicLinkStrategies.FILE)
				.loadFromRdf("/home/raven/Datasets/gitalog/store.conf.ttl")
				.connect();
			
//		dg.find(Node.ANY, Node.ANY, DCTerms.identifier.asNode(), NodeFactory.createLiteral("38a99f0e49b70f41d3774ed3127e06de01dc766f"))
//			.forEachRemaining(x -> System.out.println("Found: " + x));
		dg.find(Node.ANY, Node.ANY, DCTerms.identifier.asNode(), Node.ANY)
		.forEachRemaining(x -> System.out.println("Found: " + x));
	}

	public static void main1(String[] args) throws IOException {
		StoreDefinition sd = ModelFactory.createDefaultModel().createResource().as(StoreDefinition.class);
		
		sd.setStorePath("store");
		sd.setIndexPath("index");
		sd.addIndex("http://dataid.dbpedia.org/ns/core#group", "group", RdfTermIndexerFactoryIriToFolder.class);
		sd.addIndex("http://purl.org/dc/terms/hasVersion", "version", RdfIndexerFactoryLexicalForm.class);
		sd.addIndex(DCAT.downloadURL.asNode(), "downloadUrl", RdfTermIndexerFactoryIriToFolder.class);
		sd.addIndex(DCTerms.identifier.asNode(), "identifier", RdfIndexerFactoryLexicalForm.class);
		
		RDFDataMgr.write(System.out, sd.getModel(), RDFFormat.TURTLE_PRETTY);
	}
	
	public static void main5(String[] args) {
		Dataset ds = DatasetFactory.create();
		FusekiServer server = FusekiServer.create()
				.port(3030)
				.add("/rdf", ds)
				.build();
		System.out.println("Starting server");
		server.start();
		server.join();
		System.out.println();
	}
	
	public static void main(String[] args) throws IOException {
//		String[] a = new String[] {"a", "b"};
//		String[] b = new String[] {"a", "b"};
//
//		System.out.println(a.equals(b)); // false
//		System.out.println(Arrays.asList(a).equals(Arrays.asList(b))); // true
//		System.out.println(Array.wrap(a).equals(Array.wrap(b))); // true
		
		DatasetGraph dg = DifsFactory.newInstance()
				.setSymbolicLinkStrategy(SymbolicLinkStrategies.FILE)
				.setPath(Paths.get("/tmp/gitalog"))
				.addIndex(RDF.Nodes.type, "type", DatasetGraphIndexerFromFileSystem::uriNodeToPath)
				.addIndex(NodeFactory.createURI("http://dataid.dbpedia.org/ns/core#group"), "group", DatasetGraphIndexerFromFileSystem::uriNodeToPath)
				.addIndex(NodeFactory.createURI("http://purl.org/dc/terms/hasVersion"), "version", DatasetGraphIndexerFromFileSystem::iriOrLexicalFormToToPath)
				.addIndex(DCAT.downloadURL.asNode(), "downloadUrl", DatasetGraphIndexerFromFileSystem::uriNodeToPath)
				// .addIndex(RDF.Nodes.type, "type", DatasetGraphIndexerFromFileSystem::uriNodeToPath)
				.addIndex(DCTerms.identifier.asNode(), "identifier", DatasetGraphIndexerFromFileSystem::iriOrLexicalFormToToPath)
				.connect();
		Dataset d = DatasetFactory.wrap(dg);

		if (false) {
			Txn.executeWrite(d, () -> {
				String file = "/home/raven/Datasets/databus/dataset-per-graph.sorted.trig";
//				 String file = "/home/raven/Projects/Eclipse/cord19-rdf/rdfize/data-1000.trig";
				RDFDataMgr.read(dg, file);
			});
		}

		if (true) {
			// ISSUE: By default jena iterates all graphs
			// Can we do better with quad form algebra?
			String queryStr =
					"SELECT * { GRAPH <http://akswnc7.informatik.uni-leipzig.de/dav/dbpedia-lookup/index/2020.09.10/dataid.ttl#Dataset> {"
					+ "  ?s <http://dataid.dbpedia.org/ns/core#group> <https://databus.dbpedia.org/jan/dbpedia-lookup> ."
					+ "  ?s <http://dataid.dbpedia.org/ns/core#artifact> <https://databus.dbpedia.org/jan/dbpedia-lookup/index> ."
					+ "  ?s ?p ?o "
					+ "}}";
			System.out.println(queryStr);
			
			// String queryStr = "SELECT * { GRAPH ?g { ?s ?p ?o } } LIMIT 10";
			Query query = QueryFactory.create(queryStr);
			
			
			
			// Op op = Algebra.toQuadForm(Algebra.compile(query));
//	        Context context = ARQ.getContext().copy() ;
//	        context.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime()) ;
//	        ExecutionContext env = new ExecutionContext(context, null, null, null) ; 
//
//			QC.execute(op, BindingFactory.root(), env);
//			new QueryExecutionBase(query, d, context, null)
			
			
			// try (QueryExecution qe = QueryExecutionFactory.create(queryStr, DatasetFactory.wrap(dg))) {
			Dataset dataset = DatasetFactory.wrap(dg);
			Txn.executeRead(dataset, () -> {
				try (QueryExecution qe = QueryExecutionFactoryQuadForm.create(query, dataset)) {
					ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.SPARQLResultSetText);
				}
			});			
			
			Node p = NodeFactory.createURI("http://dataid.dbpedia.org/ns/core#group");
			Node o = NodeFactory.createURI("https://databus.dbpedia.org/jan/dbpedia-lookup");
			
//			Node s = NodeFactory.createURI("http://example.org/test");
//			dg.delete(s, s, p, o);
//			dg.add(s, s, p, o);
			
			
//			Node p = Node.ANY, DCTerms.identifier.asNode();
//			Node o = NodeFactory.createLiteral("38a99f0e49b70f41d3774ed3127e06de01dc766f")
//			dg.find(Node.ANY, Node.ANY, p, o)
//				.forEachRemaining(x -> System.out.println("Found: " + x));
		}
		if (false) {
			Txn.executeWrite(d, () -> {
				d.asDatasetGraph().delete(RDF.Nodes.first, RDF.Nodes.first, RDF.Nodes.type, RDF.Nodes.Property);
			});
		}
//

		if (false) {
			Txn.executeWrite(d, () -> {
//				d.asDatasetGraph().add(RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.Property);
//				d.asDatasetGraph().add(RDF.Nodes.first, RDF.Nodes.first, RDF.Nodes.type, RDF.Nodes.Property);
			});
		}
	
		
//		RDFDataMgr.write(System.out, d, RDFFormat.TRIG_PRETTY);
		

//		d.asDatasetGraph().find(Node.ANY, Node.ANY, RDF.Nodes.type, RDF.Nodes.Property)
//			.forEachRemaining(x -> System.out.println("Got result: " + x));
		
	}
}
