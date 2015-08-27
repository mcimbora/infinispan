package org.infinispan.jcache;

import org.jboss.arquillian.testng.Arquillian;
import org.testng.annotations.Test;

import javax.cache.Cache;
import java.lang.reflect.Method;

import static org.infinispan.jcache.JCacheCustomKeyGenerator.CustomGeneratedCacheKey;
import static org.infinispan.jcache.util.JCacheTestingUtil.getEntryCount;
import static org.testng.Assert.*;

/**
 * Base class for clustered JCache annotations tests. Implementations must provide cache & {@link
 * org.infinispan.jcache.JCacheAnnotatedClass} references.
 *
 * @author Matej Cimbora
 */
//TODO Test exception handling once implemented (e.g. cacheFor, evictFor, etc.)
@Test(testName = "org.infinispan.jcache.AbstractTwoCachesAnnotationsTest", groups = "functional")
public abstract class AbstractTwoCachesAnnotationsTest extends Arquillian {

   @Test
   public void testPut(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      getJCacheAnnotatedClass().put("put");
      assertTrue(cache1.containsKey(new CustomGeneratedCacheKey("put")));
      assertTrue(cache2.containsKey(new CustomGeneratedCacheKey("put")));
   }

   @Test
   public void testResult(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      assertEquals(getJCacheAnnotatedClass().getResultInvocationCount(), 0);

      getJCacheAnnotatedClass().result("result");
      assertEquals(getJCacheAnnotatedClass().getResultInvocationCount(), 1);

      getJCacheAnnotatedClass().result("result");
      assertEquals(getJCacheAnnotatedClass().getResultInvocationCount(), 1);

      assertTrue(cache1.containsKey(new CustomGeneratedCacheKey("result")));
      assertTrue(cache2.containsKey(new CustomGeneratedCacheKey("result")));
   }

   @Test
   public void testRemove(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      getJCacheAnnotatedClass().put("remove");
      assertTrue(cache1.containsKey(new CustomGeneratedCacheKey("remove")));
      assertTrue(cache2.containsKey(new CustomGeneratedCacheKey("remove")));

      getJCacheAnnotatedClass().remove("remove");
      assertFalse(cache1.containsKey(new CustomGeneratedCacheKey("remove")));
      assertFalse(cache2.containsKey(new CustomGeneratedCacheKey("remove")));
   }

   @Test
   public void testRemoveAll(Method m) {
      Cache cache1 = getCache1(m);
      Cache cache2 = getCache2(m);

      getJCacheAnnotatedClass().put("removeAll1");
      getJCacheAnnotatedClass().put("removeAll2");
      assertTrue(cache1.containsKey(new CustomGeneratedCacheKey("removeAll1")));
      assertTrue(cache2.containsKey(new CustomGeneratedCacheKey("removeAll1")));
      assertTrue(cache1.containsKey(new CustomGeneratedCacheKey("removeAll2")));
      assertTrue(cache2.containsKey(new CustomGeneratedCacheKey("removeAll2")));

      getJCacheAnnotatedClass().removeAll();
      assertFalse(cache1.containsKey(new CustomGeneratedCacheKey("removeAll1")));
      assertFalse(cache1.containsKey(new CustomGeneratedCacheKey("removeAll1")));
      assertFalse(cache2.containsKey(new CustomGeneratedCacheKey("removeAll2")));
      assertFalse(cache2.containsKey(new CustomGeneratedCacheKey("removeAll2")));
   }

   public abstract JCacheAnnotatedClass getJCacheAnnotatedClass();
   public abstract Cache getCache1(Method m);
   public abstract Cache getCache2(Method m);
}
