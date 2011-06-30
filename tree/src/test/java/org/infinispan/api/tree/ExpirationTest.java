package org.infinispan.api.tree;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "api.tree.ExpirationTest")
public class ExpirationTest extends SingleCacheManagerTest {
    private Log log = LogFactory.getLog(ExpirationTest.class);
    private TreeCache<String, String> cache;

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
       // start a single cache instance
       Configuration c = new Configuration();
       c.setInvocationBatchingEnabled(true);
       c.setExpirationMaxIdle(5000);

       EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c, true);

       Cache flatcache = cm.getCache();
       cache = new TreeCacheImpl(flatcache);

       return cm;
    }

    public void testDanglingChild() throws Exception {
        String testFqn = "TEST_FQN";
        String testKey = "TEST_KEY";
        String testValue = "TEST_VALUE";

        cache.put(testFqn, testKey, testValue);
        assertEquals(testValue, cache.get(testFqn, testKey));

        log.info("Sleeping for 3000 ms");
        Thread.sleep(3000);

        // Touch the child before the max-idle is reached.
        assertEquals(testValue, cache.get(testFqn, testKey));

        log.info("Sleeping for 3000 ms");
        Thread.sleep(3000);

        assertEquals(testValue, cache.get(testFqn, testKey));

        // The root should not expire because its child was touched.
        assertEquals(1, cache.getRoot().getChildren().size());
    }
}
