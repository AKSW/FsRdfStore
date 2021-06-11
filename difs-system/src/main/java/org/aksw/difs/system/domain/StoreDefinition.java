package org.aksw.difs.system.domain;

import java.util.Set;

import org.aksw.difs.sys.vocab.common.DIFSTerms;
import org.aksw.jena_sparql_api.mapper.annotation.HashId;
import org.aksw.jena_sparql_api.mapper.annotation.Iri;
import org.aksw.jena_sparql_api.mapper.annotation.ResourceView;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Resource;

@ResourceView
public interface StoreDefinition
    extends Resource
{
    @HashId
    @Iri(DIFSTerms.storePath)
    String getStorePath();
    StoreDefinition setStorePath(String path);


    @HashId
    @Iri(DIFSTerms.indexPath)
    String getIndexPath();
    StoreDefinition setIndexPath(String path);


    @Iri(DIFSTerms.index)
    Set<IndexDefinition> getIndexDefinition();

    @Iri(DIFSTerms.heartbeatInterval)
    Long getHeartbeatInterval();


    default StoreDefinition addIndex(String predicate, String folderName, Class<?> clazz) {
        return addIndex(NodeFactory.createURI(predicate), folderName, clazz);
    }

    default StoreDefinition addIndex(Node predicate, String folderName, Class<?> clazz) {
        IndexDefinition idx = getModel().createResource().as(IndexDefinition.class)
            .setPredicate(predicate)
            .setPath(folderName)
            .setMethod(clazz.getCanonicalName());

        getIndexDefinition().add(idx);

        return this;
    }
}
