package org.aksw.difs.system.domain;

import org.aksw.difs.sys.vocab.common.DIFSTerms;
import org.aksw.jena_sparql_api.mapper.annotation.HashId;
import org.aksw.jena_sparql_api.mapper.annotation.Inverse;
import org.aksw.jena_sparql_api.mapper.annotation.Iri;
import org.aksw.jena_sparql_api.mapper.annotation.ResourceView;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Resource;

/**
 * Configuration of an index. Must only be referenced by a single {@link StoreDefinition} instance.
 * 
 * @author raven
 *
 */
@ResourceView
public interface IndexDefinition
	extends Resource
{
	@Inverse
	@HashId
	StoreDefinition getStoreSpec();
	
	@Iri(DIFSTerms.predicate)
	@HashId
	Node getPredicate();
	IndexDefinition setPredicate(Node predicate);

	@Iri(DIFSTerms.folder)
	@HashId
	String getPath();
	IndexDefinition setPath(String path);

	@Iri(DIFSTerms.method)
	@HashId
	String getMethod();
	IndexDefinition setMethod(String className);
}
