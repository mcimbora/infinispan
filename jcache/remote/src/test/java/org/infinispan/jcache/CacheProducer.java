package org.infinispan.jcache;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

import java.util.Properties;

import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;

public class CacheProducer {

   @Produces
   @ApplicationScoped
   public RemoteCacheManager createCacheManager() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.addServer().host("127.0.0.1").port(15233).addServer().host("127.0.0.1").port(15234);
      return new RemoteCacheManager(cb.build());
   }
}