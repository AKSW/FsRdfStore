package org.aksw.difs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.commons.io.util.symlink.SymbolicLinkStrategies;
import org.aksw.difs.builder.DifsFactory;
import org.aksw.difs.index.impl.RdfTermIndexerFactoryIriToFolder;
import org.aksw.difs.system.domain.StoreDefinition;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.StandardSystemProperty;
import com.google.common.io.MoreFiles;


public class RollbackTests {

    @Test
    public void testForCleanIndex() throws IOException {
        Path tmpFolder = Paths.get(StandardSystemProperty.JAVA_IO_TMPDIR.value()).resolve("difs-test");
        if (Files.exists(tmpFolder)) {
            // MoreFiles.deleteRecursively(tmpFolder);
        }

        Files.createDirectories(tmpFolder);

        Path confFile = tmpFolder.resolve("store.conf.ttl");

        StoreDefinition sd = ModelFactory.createDefaultModel().createResource().as(StoreDefinition.class)
                .setStorePath("store")
                .setIndexPath("index")
                .addIndex(RDF.Nodes.type.getURI(), "type", RdfTermIndexerFactoryIriToFolder.class);

        DatasetGraph dg = DifsFactory.newInstance()
                .setSymbolicLinkStrategy(SymbolicLinkStrategies.FILE)
                .setStoreDefinition(sd)
                .setConfigFile(confFile)
                .connect();


        Quad quad = new Quad(RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type, RDF.Nodes.type);
        dg.begin();
        dg.add(quad);
        dg.delete(quad);

        dg.commit();
        //dg.abort();
        dg.end();

        List<Path> expected = Collections.singletonList(confFile);

        List<Path> actual;
        try (Stream<Path> stream = Files.list(tmpFolder)) {
            actual = stream.collect(Collectors.toList());
        }

        Assert.assertEquals(expected, actual);
    }
}
