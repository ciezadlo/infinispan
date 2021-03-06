/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.lifecycle.Lifecycle;

/**
 * A <tt>CacheContainer</tt> is the primary mechanism for retrieving a {@link org.infinispan.Cache} instance, and is often
 * used as a starting point to using the {@link org.infinispan.Cache}.
 * <p/>
 * <tt>CacheManager</tt>s are heavyweight objects, and we foresee no more than one <tt>CacheManager</tt> being used per
 * JVM (unless specific configuration requirements require more than one; but either way, this would be a minimal and
 * finite number of instances).
 * <p/>
 * You obtain <tt>Cache</tt> instances from the <tt>CacheManager</tt> by using one of the overloaded
 * <tt>getCache()</tt>, methods.  Note that with <tt>getCache()</tt>, there is no guarantee that the instance you get is
 * brand-new and empty, since caches are named and shared.  Because of this, the <tt>CacheManager</tt> also acts as a
 * repository of <tt>Cache</tt>s, and is an effective mechanism of looking up or creating <tt>Cache</tt>s on demand.
 * <p/>
 * When the system shuts down, it should call {@link #stop()} on the <tt>CacheManager</tt>.  This will ensure all caches
 * within its scope are properly stopped as well.
 * <p/>
 * <b>NB:</b> Shared caches are supported (and in fact encouraged) but if they are used it's the users responsibility to
 * ensure that <i>at least one</i> but <i>only one</i> caller calls stop() on the cache, and it does so with the awareness
 * that others may be using the cache.
 * <p />
 * Sample usage:
 * <code>
 * CacheManager manager = new DefaultCacheManager("my-config-file.xml"); 
 * Cache entityCache = manager.getCache("myEntityCache");
 * entityCache.put("aPerson", new Person());
 * <p/>
 * Configuration myNewConfiguration = new Configuration();
 * myNewConfiguration.setCacheMode(Configuration.CacheMode.LOCAL);
 * manager.defineConfiguration("myLocalCache", myNewConfiguration);
 * Cache localCache = manager.getCache("myLocalCache");
 * </code>
 *
 * @see org.infinispan.manager.EmbeddedCacheManager
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface CacheContainer extends Lifecycle {
   String DEFAULT_CACHE_NAME = "___defaultcache";

   /**
    * Retrieves the default cache associated with this cache container.
    * <p/>
    * As such, this method is always guaranteed to return the default cache.
    * <p />
    * <b>NB:</b> Shared caches are supported (and in fact encouraged) but if they are used it's the users responsibility to
    * ensure that <i>at least one</i> but <i>only one</i> caller calls stop() on the cache, and it does so with the awareness
    * that others may be using the cache.
    *
    * @return the default cache.
    */
   <K, V> Cache<K, V> getCache();

   /**
    * Retrieves a named cache from the system.  If the cache has been previously created with the same name, the running
    * cache instance is returned.  Otherwise, this method attempts to create the cache first.
    * <p/>
    * In the case of a {@link org.infinispan.manager.EmbeddedCacheManager}: when creating a new cache, this method will
    * use the configuration passed in to the EmbeddedCacheManager on construction, as a template, and then optionally
    * apply any overrides previously defined for the named cache using the {@link EmbeddedCacheManager#defineConfiguration(String, org.infinispan.config.Configuration)}
    * or {@link EmbeddedCacheManager#defineConfiguration(String, String, org.infinispan.config.Configuration)}
    * methods, or declared in the configuration file.
    * <p />
    * <b>NB:</b> Shared caches are supported (and in fact encouraged) but if they are used it's the users responsibility to
    * ensure that <i>at least one</i> but <i>only one</i> caller calls stop() on the cache, and it does so with the awareness
    * that others may be using the cache.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName
    */
   <K, V> Cache<K, V> getCache(String cacheName);
}
