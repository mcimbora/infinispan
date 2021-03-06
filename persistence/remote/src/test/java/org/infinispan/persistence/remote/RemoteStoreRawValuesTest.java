package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.internalMetadata;

import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.DefaultTimeService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(testName = "persistence.remote.RemoteStoreRawValuesTest", groups = "functional")
public class RemoteStoreRawValuesTest extends BaseStoreTest {

   private static final String REMOTE_CACHE = "remote-cache";
   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      cb.eviction().maxEntries(100).strategy(EvictionStrategy.UNORDERED)
            .expiration().wakeUpInterval(10L);

      localCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration(cb));

      localCacheManager.getCache(REMOTE_CACHE);
      hrServer = TestHelper.startHotRodServer(localCacheManager);

      RemoteStoreConfigurationBuilder storeConfigurationBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(RemoteStoreConfigurationBuilder.class)
                  .rawValues(true)
                  .remoteCacheName(REMOTE_CACHE);
      storeConfigurationBuilder
                  .addServer()
                     .host(hrServer.getHost())
                     .port(hrServer.getPort());

      RemoteStore remoteStore = new RemoteStore();
      remoteStore.init(new InitializationContextImpl(storeConfigurationBuilder.create(), getCache(), getMarshaller(),
                                                     new DefaultTimeService(), new ByteBufferFactoryImpl(),
                                                     new MarshalledEntryFactoryImpl(getMarshaller())));
      remoteStore.setInternalCacheEntryFactory(new InternalEntryFactoryImpl());
      remoteStore.start();

      return remoteStore;
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return localCacheManager.getCache("dummy").getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   @Override
   @AfterMethod
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hrServer);
      TestingUtil.killCacheManagers(localCacheManager);
   }

   @Override
   protected void assertEventuallyExpires(String key) throws Exception {
      for (int i = 0; i < 10; i++) {
         if (cl.load("k") == null) break;
         Thread.sleep(1000);
      }
      assert cl.load("k") == null;
   }

   @Override
   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(3000);
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }

   @Override
   public void testLoadAll() throws PersistenceException {
   }

   @Override
   public void testReplaceExpiredEntry() throws Exception {
      cl.write(new MarshalledEntryImpl("k1", "v1", internalMetadata(100l, null), getMarshaller()));
      // Hot Rod does not support milliseconds, so 100ms is rounded to the nearest second,
      // and so data is stored for 1 second here. Adjust waiting time accordingly.
      TestingUtil.sleepThread(1100);
      assert null == cl.load("k1");
      long start = System.currentTimeMillis();
      cl.write(new MarshalledEntryImpl("k1", "v2", internalMetadata(100l, null), getMarshaller()));
      assert cl.load("k1").getValue().equals("v2") || TestingUtil.moreThanDurationElapsed(start, 100);
   }
}

