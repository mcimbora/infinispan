package org.infinispan.client.hotrod.xsite;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.test.TestingUtil;
import org.junit.After;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;

import static org.mockito.Mockito.*;

/**
 * Reproducer for https://bugzilla.redhat.com/show_bug.cgi?id=1273795
 *
 * @author Matej Cimbora
 */
@Test(groups = "functional", testName = "xsite.ConcurrentClusterSwitchTest")
public class ConcurrentClusterSwitchTest extends AbstractHotRodSiteFailoverTest {

   // fail
   private static final int THREAD_COUNT = 50;
   // pass
//   private static final int THREAD_COUNT = 1;
   private static final int REPEAT_TIMES = 3;

   Loader[] loaders = new Loader[THREAD_COUNT];

   public void testConcurrentClusterSwitch() {
      RemoteCacheManager clientA = client(SITE_A, Optional.of(SITE_B));
      RemoteCache<String, Long> cacheA = clientA.getCache();

      int threads = THREAD_COUNT;

      for (int i = 0; i < threads; i++) {
         loaders[i] = new Loader(i, cacheA);
      }

      for (int i = 0; i < threads; i++) {
         loaders[i].start();
      }

      for (int i = 0; i < REPEAT_TIMES; i++) {
         TcpTransportFactory transportFactory = (TcpTransportFactory) TestingUtil.extractField(RemoteCacheManager.class, clientA, "transportFactory");
         Log log = mock(Log.class);
         doReturn(true).when(log).isInfoEnabled();
         replaceFinalField(log, "log", transportFactory, TcpTransportFactory.class);

         verify(log, times(0)).switchedBackToMainCluster();
         verify(log, times(0)).switchedToCluster(eq("LON"));
         verify(log, times(0)).switchedToCluster(eq("NYC"));

         int portServerSiteA = findServerPort(SITE_A);
         int portServerSiteB = findServerPort(SITE_B);
         killSite(SITE_A);

         TestingUtil.sleepThread(3000);

         verify(log, times(0)).switchedBackToMainCluster();
         verify(log, times(0)).switchedToCluster(eq("LON"));
         verify(log, times(1)).switchedToCluster(eq("NYC"));

         TestingUtil.sleepThread(3000);

         createHotRodSite(SITE_A, SITE_B, Optional.of(portServerSiteA));

         verify(log, times(0)).switchedBackToMainCluster();
         verify(log, times(0)).switchedToCluster(eq("LON"));
         verify(log, times(1)).switchedToCluster(eq("NYC"));

         killSite(SITE_B);

         TestingUtil.sleepThread(3000);

         verify(log, times(1)).switchedBackToMainCluster();
         verify(log, times(0)).switchedToCluster(eq("LON"));
         verify(log, times(1)).switchedToCluster(eq("NYC"));

         createHotRodSite(SITE_B, SITE_A, Optional.of(portServerSiteB));

         verify(log, times(1)).switchedBackToMainCluster();
         verify(log, times(0)).switchedToCluster(eq("LON"));
         verify(log, times(1)).switchedToCluster(eq("NYC"));
      }

      for (int i = 0; i < THREAD_COUNT; i++) {
         loaders[i].interrupt();
      }
   }

   private void replaceFinalField(Object newValue, String fieldName, Object owner, Class baseType) {
      Field field;
      try {
         field = baseType.getDeclaredField(fieldName);
         field.setAccessible(true);
         Field modifiers = Field.class.getDeclaredField("modifiers");
         modifiers.setAccessible(true);
         modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
         field.set(owner, newValue);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static class Loader extends Thread {

      private final int id;
      private final RemoteCache<String, Long> cache;

      public Loader(int id, RemoteCache<String, Long> cache) {
         this.id = id;
         this.cache = cache;
      }

      @Override
      public void run() {
         for (long i = 0; !Thread.currentThread().isInterrupted(); i++) {
            cache.put(i + "_" + id, i);
         }
      }
   }

   @After
   @Override
   protected void destroy() {
      for (int i = 0; i < THREAD_COUNT; i++) {
         loaders[i].interrupt();
      }
      super.destroy();
   }
}
