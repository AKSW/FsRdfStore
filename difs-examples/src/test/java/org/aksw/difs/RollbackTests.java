package org.aksw.difs;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.aksw.commons.io.util.symlink.SymbolicLinkStrategies;
import org.aksw.difs.builder.DifsFactory;
import org.aksw.difs.index.impl.RdfTermIndexerFactoryIriToFolder;
import org.aksw.difs.system.domain.StoreDefinition;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.junit.Test;

import com.google.common.base.StandardSystemProperty;

public class RollbackTests {

    @Test
    public void testForCleanIndex() throws IOException {
        Path tmpFolder = Paths.get(StandardSystemProperty.JAVA_IO_TMPDIR.value());

        StoreDefinition sd = ModelFactory.createDefaultModel().createResource().as(StoreDefinition.class)
                .setStorePath("store")
                .setIndexPath("index")
                .addIndex(RDF.Nodes.type.getURI(), "type", RdfTermIndexerFactoryIriToFolder.class);

        DatasetGraph dg = DifsFactory.newInstance()
                .setSymbolicLinkStrategy(SymbolicLinkStrategies.FILE)
                .setStoreDefinition(sd)
                .setConfigFile(tmpFolder.resolve("difs-test.ttl"))
                .connect();


        dg.begin();
        dg.add(new Quad(RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type));

        dg.abort();
    }
}
