package org.aksw.jena_sparql_api.dataset.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import org.apache.jena.ext.com.google.common.collect.Streams;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

public class DatasetGraphFromFileSystemTests {
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("/tmp/graphtest/store");
        Files.createDirectories(path);
        DatasetGraphFromFileSystem raw = DatasetGraphFromFileSystem.createDefault(path);

        raw.addPreCommitHook(dgd -> {
            System.out.println("Added:");
            RDFDataMgr.write(System.out, DatasetFactory.wrap(dgd.getAdded()), RDFFormat.TRIG_PRETTY);
            System.out.println("Removed:");
            RDFDataMgr.write(System.out, DatasetFactory.wrap(dgd.getRemoved()), RDFFormat.TRIG_PRETTY);
        });

        raw.addIndexPlugin(new DatasetGraphIndexerFromFileSystem(
                raw, DCTerms.identifier.asNode(),
                path = Paths.get("/tmp/graphtest/index/by-id"),
                DatasetGraphIndexerFromFileSystem::mavenStringToToPath
                ));

        raw.addIndexPlugin(new DatasetGraphIndexerFromFileSystem(
                raw, DCAT.distribution.asNode(),
                path = Paths.get("/tmp/graphtest/index/by-distribution"),
                DatasetGraphIndexerFromFileSystem::uriNodeToPath
                ));

        raw.addIndexPlugin(new DatasetGraphIndexerFromFileSystem(
                raw, DCAT.downloadURL.asNode(),
                path = Paths.get("/tmp/graphtest/index/by-downloadurl"),
                DatasetGraphIndexerFromFileSystem::uriNodeToPath
                ));


        DatasetGraph dg = raw;

//        Node lookupId = RDF.Nodes.type;
        Node lookupId = NodeFactory.createLiteral("my.test:id:1.0.0");
        System.out.println("Lookup results for id: ");
        dg.findNG(null, null, DCTerms.identifier.asNode(), lookupId).forEachRemaining(System.out::println);
        System.out.println("Done");


        // DcatDataset dataset = DcatDatasetCreation.fromDownloadUrl("http://my.down.load/url");
        Resource dataset = ModelFactory.createDefaultModel().createResource("http://my.down.load/url#dataset");
        dg.addGraph(dataset.asNode(), dataset.getModel().getGraph());


//        DatasetGraph dg = new DatasetGraphMonitor(raw, new DatasetChanges() {
//            @Override
//            public void start() {
//                System.out.println("start");
//            }
//
//            @Override
//            public void reset() {
//                System.out.println("reset");
//            }
//
//            @Override
//            public void finish() {
//                System.out.println("finish");
//            }
//
//            @Override
//            public void change(QuadAction qaction, Node g, Node s, Node p, Node o) {
//                System.out.println(Arrays.asList(qaction, g, s, p, o).stream()
//                        .map(Objects::toString).collect(Collectors.joining(", ")));
//            }
//        }, true);

        if (true) {
            System.out.println("graphnodes:" + Streams.stream(dg.listGraphNodes()).collect(Collectors.toList()));
//            RDFDataMgr.write(System.out, dg, RDFFormat.TRIG_BLOCKS);

    //        Txn.executeWrite(dg, () -> {
                dg.add(RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type);
    //        });

            System.out.println("Adding another graph");
    //        Txn.executeWrite(dg, () -> {
                dg.add(RDFS.Nodes.label, RDFS.Nodes.label, RDFS.Nodes.label, RDFS.Nodes.label);
                dg.add(RDFS.Nodes.label, RDFS.Nodes.label, DCTerms.identifier.asNode(), lookupId);
    //        });
        }

        Model m = RDFDataMgr.loadModel("/mnt/LinuxData/home/raven/Projects/Eclipse/dcat-suite-parent/dcat-experimental/src/main/resources/dcat-ap-ckan-mapping.ttl");
        Txn.executeWrite(dg, () -> {
            DatasetFactory.wrap(dg).addNamedModel(OWL.Class.getURI(), m);
        });

        DatasetFactory.wrap(dg).getNamedModel(OWL.Class.getURI()).add(RDF.Bag, RDF.type, RDF.Bag);
        dg.add(OWL.Class.asNode(), RDFS.Nodes.label, DCTerms.identifier.asNode(), lookupId);

        System.out.println("done");
    }

}