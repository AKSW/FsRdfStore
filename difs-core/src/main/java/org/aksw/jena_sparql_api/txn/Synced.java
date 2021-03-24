package org.aksw.jena_sparql_api.txn;

import java.time.Instant;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** An instance T backed by an entity E. The entity is typically a file. */
public class Synced<E, T>
{
	protected E entity;
	protected BiConsumer<E, T> saver;
	protected Function<E, T> loader;
	protected Function<E, Instant> getLastModifiedDate;
	
	public Synced(
			E entity,
			Function<E, T> loader,
			BiConsumer<E, T> saver,
			Function<E, Instant> getLastModifiedDate) {
		super();
		this.entity = entity;
		this.saver = saver;
		this.loader = loader;
		this.getLastModifiedDate = getLastModifiedDate;
	}

	protected T instance;
	protected Instant lastModifiedDate;
	
	public E getEntity() {
		return entity;
	}
	
	public void load() {
		lastModifiedDate = getLastModifiedDate.apply(entity);
		instance = loader.apply(entity);
	}
	
	public T get() {
		if (instance == null) {
			load();
		}
		
		return instance;
	}
	
//	public Synced set(T instance) {
//		this.instance = instance;
//	}
	
	public void save() {
		saver.accept(entity, instance);
	}
}
	
