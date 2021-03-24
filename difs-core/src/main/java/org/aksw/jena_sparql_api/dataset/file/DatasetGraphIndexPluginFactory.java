package org.aksw.jena_sparql_api.dataset.file;

import org.aksw.jena_sparql_api.txn.DatasetGraphWithSyncOld;

public interface DatasetGraphIndexPluginFactory {
    DatasetGraphIndexPlugin create(DatasetGraphWithSyncOld graphWithSync);
}
