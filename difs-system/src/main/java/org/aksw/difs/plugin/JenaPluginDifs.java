package org.aksw.difs.plugin;

import org.aksw.difs.system.domain.IndexDefinition;
import org.aksw.difs.system.domain.StoreDefinition;
import org.aksw.jena_sparql_api.mapper.proxy.JenaPluginUtils;
import org.apache.jena.sys.JenaSubsystemLifecycle;

public class JenaPluginDifs
	implements JenaSubsystemLifecycle {

	public void start() {
	    init();
	}
	
	@Override
	public void stop() {
	}


	public static void init() {
	    JenaPluginUtils.registerResourceClasses(
	    	StoreDefinition.class,
	    	IndexDefinition.class
	    );
	
	}
}
