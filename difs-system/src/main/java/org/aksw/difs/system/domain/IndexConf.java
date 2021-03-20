package org.aksw.difs.system.domain;

import org.aksw.jena_sparql_api.mapper.annotation.Inverse;
import org.aksw.jena_sparql_api.mapper.annotation.IriNs;
import org.aksw.jena_sparql_api.mapper.annotation.Namespace;
import org.aksw.jena_sparql_api.mapper.annotation.ResourceView;

/**
 * Configuration of an index. Must only be referenced by a single {@link StoreSpec} instance.
 * 
 * @author raven
 *
 */
@ResourceView
@Namespace(prefix = "difs", value = "http://www.example.org/")
public interface IndexConf {
	@Inverse
	StoreSpec getStoreSpec();
	
	@IriNs("difs")
	String getPath();
	StoreSpec setPath(String path);

	@IriNs("difs")
	String getJavaClassName();
	StoreSpec setJavaClassName(String className);
}
