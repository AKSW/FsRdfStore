package org.aksw.difs.index.impl;

import java.nio.file.Path;
import java.util.function.Function;

import org.aksw.difs.index.api.RdfTermIndexerFactory;
import org.aksw.jena_sparql_api.dataset.file.DatasetGraphIndexerFromFileSystem;
import org.apache.jena.graph.Node;

public class RdfIndexerFactoryLexicalForm
	implements RdfTermIndexerFactory
{
	@Override
	public Function<Node, Path> getMapper() {
		return DatasetGraphIndexerFromFileSystem::iriOrLexicalFormToToPath;
	}
}
