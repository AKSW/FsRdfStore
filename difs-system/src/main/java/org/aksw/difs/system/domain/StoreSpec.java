package org.aksw.difs.system.domain;

import java.util.Set;

import org.aksw.jena_sparql_api.mapper.annotation.IriNs;
import org.aksw.jena_sparql_api.mapper.annotation.Namespace;
import org.aksw.jena_sparql_api.mapper.annotation.ResourceView;

@ResourceView
@Namespace(prefix = "difs", value = "http://www.example.org/")
public interface StoreSpec {
	@IriNs("difs")
	String getPath();
	StoreSpec setPath(String path);
	
	@IriNs("difs")
	Set<IndexConf> getIndexConfs();
	
}
