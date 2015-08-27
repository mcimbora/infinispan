package org.infinispan.jcache;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.jcache.annotation.InjectedCachePutInterceptor;
import org.infinispan.jcache.embedded.JCache;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.jcache.embedded.annotation.EmbeddedInjectedCacheResolver;
import org.infinispan.jcache.util.JCacheTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.cache.Cache;
import javax.cache.Caching;
import javax.inject.Inject;
import java.lang.reflect.Method;
import java.net.URI;

import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;

/**
 * @author Matej Cimbora
 */
@Test(testName = "org.infinispan.jcache.JCacheTwoCachesAnnotationsTest", groups = "functional")
public class JCacheTwoCachesAnnotationsTest extends AbstractTwoCachesAnnotationsTest {

   private static EmbeddedCacheManager cacheManager1;
   private static EmbeddedCacheManager cacheManager2;
   private static Cache cache1;
   private static Cache cache2;

   @Deployment
   public static JavaArchive createDeployment() {
      return ShrinkWrap.create(JavaArchive.class).addPackage(JCacheAnnotatedClass.class.getPackage()).addPackage(JCache.class.getPackage())
            .addPackage(EmbeddedInjectedCacheResolver.class.getPackage()).addPackage(InjectedCachePutInterceptor.class.getPackage()).addPackage(CacheProducer.class.getPackage()).addPackage(JCacheTestingUtil.class.getPackage())
            .addAsResource(JCacheTwoCachesAnnotationsTest.class.getResource("/beans.xml"), "beans.xml");
   }

   @Inject
   private JCacheAnnotatedClass jCacheAnnotatedClass;

   @Override
   public JCacheAnnotatedClass getJCacheAnnotatedClass() {
      return jCacheAnnotatedClass;
   }

   @Override
   public Cache getCache1(Method m) {
      return cache1;
   }

   @Override
   public Cache getCache2(Method m) {
      return cache2;
   }

   @BeforeClass
   public void initCacheManagers() {
      cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      cacheManager1.defineConfiguration("annotation", getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC).build());
      cache1 = new JCacheManager(URI.create(JCacheTwoCachesAnnotationsTest.class.getName()), cacheManager1, null).getCache("annotation");

      cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      cacheManager2.defineConfiguration("annotation", getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC).build());
      cache2 = new JCacheManager(URI.create(JCacheTwoCachesAnnotationsTest.class.getName()), cacheManager2, null).getCache("annotation");

      TestingUtil.blockUntilViewsReceived(30000, cacheManager1.getCache("annotation"), cacheManager2.getCache("annotation"));
   }

   @AfterClass
   protected void destroy() {
      TestingUtil.killCacheManagers(cacheManager1, cacheManager2);
   }

   @AfterMethod
   public void clearCaches() {
      cache1.clear();
      cache2.clear();
   }
}
