package org.aksw.difs.index.api;

import org.aksw.jena_sparql_api.dataset.file.DatasetGraphIndexPlugin;
import org.apache.jena.rdf.model.Resource;

// Not (yet) used
public interface IndexFactory {
	DatasetGraphIndexPlugin createIndex(Resource state);
}
