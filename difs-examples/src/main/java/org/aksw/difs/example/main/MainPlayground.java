package org.aksw.difs.example.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.aksw.commons.io.block.impl.BlockSources;
import org.aksw.commons.io.util.symlink.SymbolicLinkStrategies;
import org.aksw.difs.builder.DifsFactory;
import org.aksw.difs.engine.QueryEngineQuadForm;
import org.aksw.difs.engine.QueryExecutionFactoryQuadForm;
import org.aksw.difs.engine.UpdateProcessorFactoryQuadForm;
import org.aksw.difs.index.impl.RdfIndexerFactoryLexicalForm;
import org.aksw.difs.index.impl.RdfTermIndexerFactoryIriToFolder;
import org.aksw.difs.system.domain.StoreDefinition;
import org.aksw.jena_sparql_api.core.QueryExecutionFactoryDataset;
import org.aksw.jena_sparql_api.core.SparqlService;
import org.aksw.jena_sparql_api.core.SparqlServiceFactory;
import org.aksw.jena_sparql_api.core.SparqlServiceImpl;
import org.aksw.jena_sparql_api.core.UpdateExecutionFactoryDataset;
import org.aksw.jena_sparql_api.io.binseach.BinarySearcher;
import org.aksw.jena_sparql_api.rx.RDFDataMgrEx;
import org.aksw.jena_sparql_api.rx.entity.EntityInfo;
import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer;
import org.aksw.jena_sparql_api.sparql.ext.fs.OpExecutorServiceOrFile;
import org.aksw.jena_sparql_api.stmt.SparqlParserConfig;
import org.aksw.jena_sparql_api.stmt.SparqlQueryParser;
import org.aksw.jena_sparql_api.stmt.SparqlQueryParserImpl;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.http5.Http5FileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.webdav.WebdavFileSystemConfigBuilder;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFWriter;
import org.apache.jena.riot.RIOT;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.system.Txn;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.eclipse.jetty.server.Server;

import com.google.common.base.Stopwatch;
import com.sshtools.vfs2nio.Vfs2NioFileSystemProvider;

public class MainPlayground {

    public static void main(String[] args) throws Exception {

        // Dataset m = RDFDataMgr.loadDataset("/var/www/webdav/gitalog/store/org.mclient.foobar.baz/data.trig");
//        String str = "/var/www/webdav/gitalog/store/org.mclient.foobar.baz/data.trig";
//
//
//        Dataset m = RDFDataMgrEx.loadDatasetAsGiven(str);
//        RDFDataMgr.write(System.out, m, RDFFormat.TRIG_BLOCKS);

        // mainX(args);
        parseTest();
    }

    public static void parseTest() {
        SparqlQueryParser parser = SparqlQueryParserImpl.create(SparqlParserConfig.newInstance()
                .setIrixResolverAsGiven());
        Query q = parser.apply("SELECT * { GRAPH <" + "test" + "> { ?s <http://www.w3.org/2000/01/rdf-schema#member> ?o } }");

        System.out.println(q);

    }

    public static void mainZ(String[] args) throws Exception {

        if (true) {
            for (int i = 0; i < 10; ++i) {
                String file = "/home/raven/tmp/data.nt.bz2";
                file = "/home/raven/tmp/corrupted.nt.bz2";
                try (InputStream in = Files.newInputStream(Paths.get(file))) {
                    EntityInfo info = RDFDataMgrEx.probeEntityInfo(in, RDFDataMgrEx.DEFAULT_PROBE_LANGS);
                    System.out.println(info);
                }
            }
            return;
        }


        QC.setFactory(ARQ.getContext(), execCxt -> {
//                execCxt.getContext().set(ARQ.stageGenerator, StageBuilder.executeInline);
            return new OpExecutorServiceOrFile(execCxt);
        });

        String queryStr;

        System.out.println("http".replaceAll("^http(?!\\d+)", "http4"));
        System.out.println("https".replaceAll("^http(?!\\d+)", "http4"));
        System.out.println("http3".replaceAll("^http(?!\\d+)", "http4"));
        System.out.println("http3s".replaceAll("^http(?!\\d+)", "http4"));


//        queryStr = "SELECT * { SERVICE <x-binsearch:vfs:http4://localhost/webdav/dnb-all_lds_20200213.sorted.nt.bz2> { <https://d-nb.info/1000000028> ?p ?o . } }";
//        queryStr = "SELECT * { SERVICE <x-binsearch:vfs:http4://localhost/webdav/dnb-all_lds_20200213.sorted.nt.bz2> { <https://d-nb.info/1000000028> ?p ?o . ?o ?x ?y} }";
        // String queryStr = "SELECT * { SERVICE <x-binsearch:vfs:http4s://databus.dbpedia.org/data/databus/databus-data/2019.10.20/databus-data.nt.bz2> { <http://akswnc7.informatik.uni-leipzig.de/dstreitmatter/dbpedia-diff/labels-diff/2019.09.02/dataid.ttl#Dataset> ?p ?o . } }";
        // String queryStr = "SELECT * { SERVICE <x-binsearch:file:///home/raven/Datasets/databus-data.nt.bz2> { <http://akswnc7.informatik.uni-leipzig.de/dstreitmatter/dbpedia-diff/labels-diff/2019.09.02/dataid.ttl#Dataset> ?p ?o . } }";


        // first
        queryStr = "SELECT * { SERVICE <x-binsearch:vfs:https://databus.dbpedia.org/dnkg/cartridge-input/kb/2020.09.29/kb_partition=person_set=thes_content=facts_origin=export.nt.bz2> { <http://data.bibliotheken.nl/id/thes/p067460208> ?p ?o } }";

        // last
//        queryStr = "SELECT * { SERVICE <x-binsearch:vfs:http4s://databus.dbpedia.org/dnkg/cartridge-input/kb/2020.09.29/kb_partition=person_set=thes_content=facts_origin=export.nt.bz2> { <http://data.bibliotheken.nl/id/thes/p428736572> ?p ?o } }";

        // middle
//        queryStr = "SELECT * { SERVICE <x-binsearch:vfs:http4s://databus.dbpedia.org/dnkg/cartridge-input/kb/2020.09.29/kb_partition=person_set=thes_content=facts_origin=export.nt.bz2> { <http://data.bibliotheken.nl/id/thes/p153093994> ?p ?o } }";


         queryStr = "SELECT * { SERVICE <x-binsearch:vfs:https://downloads.dbpedia.org/repo/dbpedia/text/nif-page-structure/2020.02.01/nif-page-structure_lang=bg.ttl.bz2> { <http://bg.dbpedia.org/resource/Европейско_първенство_по_волейбол_за_жени_2011?dbpv=2020-02&nif=section&char=238,416> ?p ?o . } }";

        for (int i = 0; i < 10; ++i) {

        Stopwatch sw = Stopwatch.createStarted();
        Dataset dataset = DatasetFactory.create();
        try (QueryExecution qe = QueryExecutionFactory.create(queryStr, dataset)) {
            ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_Text);
        }
        System.out.println("Time taken: " + sw.elapsed(TimeUnit.MILLISECONDS) * 0.001f);

        }

    }

    public static void mainTestNoBase(String[] args) throws Exception {
        Model model = ModelFactory.createDefaultModel();
        model.add(RDF.type, RDF.type, RDF.Property);

        RDFWriter writer = RDFWriter.create()
                .format(RDFFormat.TURTLE_PRETTY)
                .base(RDF.uri)
                .set(RIOT.symTurtleOmitBase, true)
                .source(model)
                .build();

        writer.output(System.out);

        // Output: <#type>  a      <#Property> .
    }


    public static void mainBinSearch(String[] args) throws Exception {

        FileSystemOptions fsOpts = new FileSystemOptions();
        Http5FileSystemConfigBuilder.getInstance().setKeepAlive(fsOpts, false);

        Map<String, Object> env = new HashMap<>();
        env.put(Vfs2NioFileSystemProvider.FILE_SYSTEM_OPTIONS, fsOpts);


        FileSystem fs = FileSystems.newFileSystem(
                URI.create("vfs:" + "http5://localhost/"),
                env);
//
//		// fs.getRootDirectories().iterator().next().resolve("aksw.org/robots.txt");
        Path path = fs.getRootDirectories().iterator().next()
                .resolve("webdav/dnb-all_lds_20200213.sorted.nt.bz2");

//		Path path = Paths.get(new URI("vfs:" + "http://localhost/webdav/dnb-all_lds_20200213.sorted.nt.bz2"));

        System.out.println(path.toUri());
        try (BinarySearcher bs = BlockSources.createBinarySearcherBz2(path, 32 * 1024)) {
            InputStream in = bs.search("<https://d-nb.info/1000000028>");
            new BufferedReader(new InputStreamReader(in)).lines().forEach(System.out::println);
        }
    }

    public static void main3(String[] args) throws IOException {

        DatasetGraph dg = DifsFactory.newInstance()
                .setSymbolicLinkStrategy(SymbolicLinkStrategies.FILE)
                .setConfigFile(Paths.get("/home/raven/Datasets/gitalog/store.conf.ttl"))
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


    public static void mainVfsHttpTest(String[] args) throws Exception {
        String url = "http5://localhost/webdav/dnb-all_lds_20200213.sorted.nt.bz2";
        FileSystemManager fsManager = VFS.getManager();

        Random rand = new Random();
        try (FileObject file = fsManager.resolveFile(url)) {
            try (RandomAccessContent r = file.getContent().getRandomAccessContent(RandomAccessMode.READ)) {

                for (int i = 0; i < 100000; ++i) {
                    long pos = rand.nextInt(1000000000);
                    StopWatch sw = StopWatch.createStarted();
                    r.seek(pos);
                    byte[] bytes = new byte[100];
                    r.readFully(bytes);
                    System.out.println("Read at " + pos + " took " + sw.getTime(TimeUnit.MILLISECONDS));
                    // System.out.println(new String(bytes));
                }
            }
        }
        System.out.println("Done");
    }

    public static void mainFileChannelHttp(String[] args) throws Exception {
        FileSystem fs = FileSystems.newFileSystem(
                URI.create("vfs:" + "http://aksw.org"),
                null);

        // fs.getRootDirectories().iterator().next().resolve("aksw.org/robots.txt");
        Path path = fs.getRootDirectories().iterator().next()
                .resolve("robots.txt");

        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ)) {
            fc.position(20);
            byte[] bytes = new byte[100];
            fc.read(ByteBuffer.wrap(bytes));
            System.out.println(new String(bytes));
        }

        try (InputStream in = Files.newInputStream(path)) {
            byte[] bytes = new byte[100];
            in.read(bytes);
            System.out.println(new String(bytes));
        }
    }


    public static void mainX(String[] args) throws Exception {
        JenaSystem.init();

        String[] vfsConfWebDav = new String[]{"webdav://localhost", "webdav/gitalog/store.conf.ttl"};
        String[] vfsConfLocalFs = new String[]{"file:///", "/var/www/webdav/gitalog/store.conf.ttl"};
        String[] vfsConfZip = new String[]{"zip:///tmp/gitalog/gitalog.zip", "store.conf.ttl"};


        boolean useJournal = true;
        String[] vfsConf = vfsConfLocalFs;


//		String vfsUri = "file:///var/www/webdav/gitalog/store.conf";
//		String vfsUri = "zip:///tmp/gitalog/gitalog.zip";
        FileSystemOptions webDavFsOpts = new FileSystemOptions();
        WebdavFileSystemConfigBuilder.getInstance().setFollowRedirect(webDavFsOpts, false);

        Map<String, Object> env = new HashMap<>();
        env.put(Vfs2NioFileSystemProvider.FILE_SYSTEM_OPTIONS, webDavFsOpts);

        String vfsUri = vfsConf[0];
        FileSystem fs;

        if (vfsUri.startsWith("file:")) {
            fs = Paths.get("/").getFileSystem();
        } else {
            fs = FileSystems.newFileSystem(
                    URI.create("vfs:" + vfsUri),
                    env);
        }
        // zip file
//		Path basePath = fs.getRootDirectories().iterator().next().resolve("store.conf.ttl");
//		Path basePath = Paths.get("/tmp/gitalog/store.conf");
//		Path basePath = fs.getRootDirectories().iterator().next()
//				 .resolve("var").resolve("www")
//				.resolve("webdav").resolve("gitalog");

        Path basePath = fs.getRootDirectories().iterator().next();
        for (int i = 1; i < vfsConf.length; ++i) {
            String segment = vfsConf[i];
            basePath = basePath.resolve(segment);
        }

        StoreDefinition sd = ModelFactory.createDefaultModel().createResource().as(StoreDefinition.class)
                .setStorePath("store")
                .setIndexPath("index")
                .addIndex("http://dataid.dbpedia.org/ns/core#group", "group", RdfTermIndexerFactoryIriToFolder.class)
                .addIndex("http://purl.org/dc/terms/hasVersion", "version", RdfIndexerFactoryLexicalForm.class)
                .addIndex(DCAT.downloadURL.asNode(), "downloadUrl", RdfTermIndexerFactoryIriToFolder.class)
                .addIndex(DCTerms.identifier.asNode(), "identifier", RdfIndexerFactoryLexicalForm.class);

        DatasetGraph dg = DifsFactory.newInstance()
                .setStoreDefinition(sd)
                .setUseJournal(useJournal)
                .setSymbolicLinkStrategy(SymbolicLinkStrategies.FILE)
                .setConfigFile(basePath)
                .setMaximumNamedGraphCacheSize(10000)
//				.addIndex(RDF.Nodes.type, "type", DatasetGraphIndexerFromFileSystem::uriNodeToPath)
//				.addIndex(NodeFactory.createURI("http://dataid.dbpedia.org/ns/core#group"), "group", DatasetGraphIndexerFromFileSystem::uriNodeToPath)
//				.addIndex(NodeFactory.createURI("http://purl.org/dc/terms/hasVersion"), "version", DatasetGraphIndexerFromFileSystem::iriOrLexicalFormToToPath)
//				.addIndex(DCAT.downloadURL.asNode(), "downloadUrl", DatasetGraphIndexerFromFileSystem::uriNodeToPath)
//				// .addIndex(RDF.Nodes.type, "type", DatasetGraphIndexerFromFileSystem::uriNodeToPath)
//				.addIndex(DCTerms.identifier.asNode(), "identifier", DatasetGraphIndexerFromFileSystem::iriOrLexicalFormToToPath)
                .connect();
        Dataset d = DatasetFactory.wrap(dg);

        if (false) {
            Txn.executeWrite(d, () -> {
                d.asDatasetGraph().delete(RDF.Nodes.first, RDF.Nodes.first, RDF.Nodes.type, RDF.Nodes.Property);
            });
        }

        if (false) {
            Txn.executeWrite(d, () -> {
                String file = "/home/raven/Datasets/databus/dataset-per-graph.sorted.trig";
//				 String file = "/home/raven/Projects/Eclipse/cord19-rdf/rdfize/data-1000.trig";
                RDFDataMgr.read(dg, file);
            });
        }

        if (true) {
            String queryStr;
            queryStr =
                    "SELECT * { GRAPH ?g {"
                    + "  ?s <http://dataid.dbpedia.org/ns/core#group> <https://databus.dbpedia.org/jan/dbpedia-lookup> ."
                    + "  ?s <http://dataid.dbpedia.org/ns/core#artifact> <https://databus.dbpedia.org/jan/dbpedia-lookup/index> ."
                    + "  ?s ?p ?o "
                    + "}}";
//			String queryStr =
//					"SELECT * { GRAPH <http://akswnc7.informatik.uni-leipzig.de/dav/dbpedia-lookup/index/2020.09.10/dataid.ttl#Dataset> {"
//					+ "  ?s <http://dataid.dbpedia.org/ns/core#group> <https://databus.dbpedia.org/jan/dbpedia-lookup> ."
//					+ "  ?s <http://dataid.dbpedia.org/ns/core#artifact> <https://databus.dbpedia.org/jan/dbpedia-lookup/index> ."
//					+ "  ?s ?p ?o "
//					+ "}}";

//			queryStr = "SELECT DISTINCT ?t { GRAPH ?g { ?s a ?t } }";
            //queryStr = "SELECT * { GRAPH ?g { ?s ?p ?o } } LIMIT 10";
            System.out.println(queryStr);

            Query query = QueryFactory.create(queryStr);



            // Op op = Algebra.toQuadForm(Algebra.compile(query));
//	        Context context = ARQ.getContext().copy() ;
//	        context.set(ARQConstants.sysCurrentTime, NodeFactoryExtra.nowAsDateTime()) ;
//	        ExecutionContext env = new ExecutionContext(context, null, null, null) ;
//
//			QC.execute(op, BindingFactory.root(), env);
//			new QueryExecutionBase(query, d, context, null)


            // try (QueryExecution qe = QueryExecutionFactory.create(queryStr, DatasetFactory.wrap(dg))) {

            if (true) {
                Dataset dataset = DatasetFactory.wrap(dg);
                Txn.executeRead(dataset, () -> {
                    try (QueryExecution qe = QueryExecutionFactoryQuadForm.create(query, dataset)) {
                        ResultSetMgr.write(System.out, qe.execSelect(), ResultSetLang.RS_Text);
                    }
                });
            }

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
//

        if (false) {
            Txn.executeWrite(d, () -> {
//				d.asDatasetGraph().add(RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.Property);
//				d.asDatasetGraph().add(RDF.Nodes.first, RDF.Nodes.first, RDF.Nodes.type, RDF.Nodes.Property);
            });
        }


        SparqlService ss = new SparqlServiceImpl(
                new QueryExecutionFactoryDataset(d, null, (qu, da, co) -> QueryEngineQuadForm.factory),
                new UpdateExecutionFactoryDataset(d, null, UpdateProcessorFactoryQuadForm::create));

        SparqlServiceFactory ssf = (uri, dd, httpClient) -> ss;

        Server server = FactoryBeanSparqlServer.newInstance()
                .setPort(7531)
                .setSparqlServiceFactory(ssf)
                .create();

//		FusekiServer server = FusekiServer.create()
//				.port(3030)
//				.add("/rdf", d)
//				.enableCors(true)
//				.build();
        System.out.println("Starting server");
        server.start();
        server.join();
        System.out.println();

//		RDFDataMgr.write(System.out, d, RDFFormat.TRIG_PRETTY);


//		d.asDatasetGraph().find(Node.ANY, Node.ANY, RDF.Nodes.type, RDF.Nodes.Property)
//			.forEachRemaining(x -> System.out.println("Got result: " + x));

    }
}
