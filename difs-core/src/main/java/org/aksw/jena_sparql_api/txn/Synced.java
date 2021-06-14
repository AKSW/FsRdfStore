package org.aksw.jena_sparql_api.txn;

/** An instance T backed by an entity E. The entity is typically a file. */
//public class Synced<E, T>
//{
//	protected E entity;
//	protected BiConsumer<E, T> saver;
//	protected Function<E, T> loader;
//	protected Function<E, Instant> getLastModifiedDate;
//
//	protected T instance;
//	protected Instant lastModifiedDate;
//
//	// Saving a non-dirty entity is a no-op
//	protected boolean isDirty;
//
//
//	public Synced(
//			E entity,
//			Function<E, T> loader,
//			BiConsumer<E, T> saver,
//			Function<E, Instant> getLastModifiedDate) {
//		super();
//		this.entity = entity;
//		this.saver = saver;
//		this.loader = loader;
//		this.getLastModifiedDate = getLastModifiedDate;
//	}
//
//	public T getOriginalState() {
//
//	}
//
//	public T getCurrentState() {
//
//	}
//
//	public E getEntity() {
//		return entity;
//	}
//
//	public void load() {
//		lastModifiedDate = getLastModifiedDate.apply(entity);
//		instance = loader.apply(entity);
//	}
//
//	public T get() {
//		if (instance == null) {
//			load();
//		}
//
//		return instance;
//	}
//
//	public Synced setDirty(boolean isDirty) {
//		this.isDirty = isDirty;
//		return this;
//	}
////	public Synced set(T instance) {
////		this.instance = instance;
////	}
//
//	public void save() {
//		if (isDirty) {
//			saver.accept(entity, instance);
//		}
//	}
//}
//
