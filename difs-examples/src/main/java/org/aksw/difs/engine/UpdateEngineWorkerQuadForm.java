package org.aksw.difs.engine;

import java.util.Iterator;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.modify.UpdateEngineWorker;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.util.Context;

public class UpdateEngineWorkerQuadForm
	extends UpdateEngineWorker
{
	public UpdateEngineWorkerQuadForm(DatasetGraph datasetGraph, Binding inputBinding, Context context) {
		super(datasetGraph, inputBinding, context);
	}

    protected Iterator<Binding> evalBindings(Element pattern) {
        Query query = elementToQuery(pattern);
        return evalBindings(query, datasetGraph, inputBinding, context);
    }

//    protected static Iterator<Binding> evalBindings(Query query, DatasetGraph dsg, Binding inputBinding, Context context) {
//        // The UpdateProcessorBase already copied the context and made it safe
//        // ... but that's going to happen again :-(
//
//        Iterator<Binding> toReturn;
//
//        if ( query != null ) {
//            Plan plan = QueryExecutionFactory.createPlan(query, dsg, inputBinding, context);
//            toReturn = plan.iterator();
//        } else {
//            toReturn = Iter.singleton((null != inputBinding) ? inputBinding : BindingRoot.create());
//        }
//        return toReturn;
//    }	
}
