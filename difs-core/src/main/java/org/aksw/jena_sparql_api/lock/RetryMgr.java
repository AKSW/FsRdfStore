package org.aksw.jena_sparql_api.lock;

import java.util.List;

/* A retry manager that first acquires a set of resources; where resources may get 'lost' due to concurrent modifications.
   For example, a lost resource might be a folder which got deleted by another process.
*/

interface Res
	extends AutoCloseable {
	void acquire();
	boolean isAcquired();
}

public interface RetryMgr {
	void add(Res res);
	void run();
}

class RetryMgrImpl
	implements RetryMgr
{
	protected List<Res> resources;
	protected boolean releaseAllResourcesOnError;

	@Override
	public void add(Res resource) {
		resources.add(resource);
	}

	protected void scheduleRetry() {
		
	}
	
	@Override
	public void run() {
		try {
			for (Res resource : resources) {
				resource.acquire();
			}
		} catch (Exception e) {
			// If a resource has been lost then schedule a retry
			boolean allAcquired = true;
			for (Res resource : resources) {
				allAcquired = allAcquired || resource.isAcquired();
			}
			
			if (!allAcquired) {
				scheduleRetry();
			}
			
		} finally {
			
			for (Res resource : resources) {
				// resource.close();
			}			
		}
	
	}
	
	
}