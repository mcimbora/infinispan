package org.infinispan.jcache;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.jcache.annotation.InjectedCachePutInterceptor;
//import org.infinispan.jcache.embedded.JCache;
//import org.infinispan.jcache.embedded.JCacheManager;
//import org.infinispan.jcache.embedded.annotation.EmbeddedInjectedCacheResolver;
import org.infinispan.jcache.remote.JCache;
import org.infinispan.jcache.remote.annotation.RemoteInjectedCacheResolver;
import org.infinispan.jcache.util.JCacheTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
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
import java.util.Properties;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.jcache.util.JCacheTestingUtil.createCacheWithProperties;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;

/**
 * @author Matej Cimbora
 */
@Test(testName = "org.infinispan.jcache.JCacheTwoCachesAnnotationsTest", groups = "functional")
public class JCacheTwoCachesAnnotationsTest extends AbstractTwoCachesAnnotationsTest {

   public static HotRodServer hotRodServer1;
   public static HotRodServer hotRodServer2;
   private static Cache cache1;
   private static Cache cache2;
   private ClassLoader testSpecificClassLoader;

   @Deployment
   public static JavaArchive createDeployment() {
      return ShrinkWrap.create(JavaArchive.class).addPackage(JCacheAnnotatedClass.class.getPackage()).addPackage(JCache.class.getPackage())
            .addPackage(RemoteInjectedCacheResolver.class.getPackage()).addPackage(InjectedCachePutInterceptor.class.getPackage()).addPackage(CacheProducer.class.getPackage()).addPackage(JCacheTestingUtil.class.getPackage())
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
   protected void createCacheManagers() throws Throwable {

      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      cacheManager1.defineConfiguration("annotation", getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC).build());
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      cacheManager2.defineConfiguration("annotation", getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC).build());

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(cacheManager1);
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(cacheManager2);
      testSpecificClassLoader = new JCacheTestingUtil.TestClassLoader(JCacheTwoCachesBasicOpsTest.class.getClassLoader());

      Properties properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer1.getHost() + ":" + hotRodServer1.getPort());
      cache1 = createCacheWithProperties(Caching.getCachingProvider(testSpecificClassLoader), JCacheTwoCachesBasicOpsTest.class, "annotation", properties);

      properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer2.getHost() + ":" + hotRodServer2.getPort());
      cache2 = createCacheWithProperties(Caching.getCachingProvider(testSpecificClassLoader), JCacheTwoCachesBasicOpsTest.class, "annotation", properties);

   }

   @AfterClass
   protected void destroy() {
      killServers(hotRodServer1, hotRodServer2);
      Caching.getCachingProvider(testSpecificClassLoader).close();
   }

   @AfterMethod
   protected void clean() {
      cache1.clear();
      cache2.clear();
   }
}
