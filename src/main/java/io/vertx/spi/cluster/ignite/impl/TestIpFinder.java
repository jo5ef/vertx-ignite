package io.vertx.spi.cluster.ignite.impl;

import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TestIpFinder implements TcpDiscoveryIpFinder {

  static final List<InetSocketAddress> addresses = new ArrayList<>();

  @Override
  public void onSpiContextInitialized(IgniteSpiContext igniteSpiContext) throws IgniteSpiException {

  }

  @Override
  public void onSpiContextDestroyed() {

  }

  @Override
  public void initializeLocalAddresses(Collection<InetSocketAddress> collection) throws IgniteSpiException {
    addresses.addAll(collection);
  }

  @Override
  public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
    return addresses;
  }

  @Override
  public boolean isShared() {
    return true;
  }

  @Override
  public void registerAddresses(Collection<InetSocketAddress> collection) throws IgniteSpiException {

  }

  @Override
  public void unregisterAddresses(Collection<InetSocketAddress> collection) throws IgniteSpiException {
    addresses.removeAll(collection);
  }

  @Override
  public void close() {
  }
}
