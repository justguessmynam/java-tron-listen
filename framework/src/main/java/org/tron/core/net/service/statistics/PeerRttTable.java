package org.tron.core.net.service.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 维护已连接 Peer 的最近两次 RTT（Round Trip Time）采样值。
 * 由采集线程定期写入，业务线程按需读取，线程安全。
 */
public class PeerRttTable {

  /**
   * 单条 Peer 的 RTT 记录：当前值 + 上一次值
   */
  public static class RttPair {
    public long current;
    public long previous;

    public RttPair(long current, long previous) {
      this.current = current;
      this.previous = previous;
    }

    public long getCurrent() {
      return current;
    }

    public long getPrevious() {
      return previous;
    }
  }

  private final ConcurrentHashMap<String, RttPair> table = new ConcurrentHashMap<>();

  /**
   * 写入或更新一个 Peer 的 RTT 值。
   * 新值成为 current，旧 current 下沉为 previous。
   * 首次写入时 previous 为 -1。
   *
   * @param ip  Peer IP 地址
   * @param rtt 本轮采集的平均 RTT，单位毫秒
   */
  public void put(String ip, long rtt) {
    table.merge(ip, new RttPair(rtt, -1),
        (oldPair, newPair) -> new RttPair(rtt, oldPair.current));
  }

  /**
   * 获取指定 Peer 的最新 RTT（即 current）
   *
   * @param ip Peer IP 地址
   * @return current 毫秒值，不存在时返回 -1
   */
  public long getRtt(String ip) {
    RttPair pair = table.get(ip);
    return pair != null ? pair.current : -1L;
  }

  /**
   * 获取指定 Peer 的当前 RTT
   *
   * @param ip Peer IP 地址
   * @return current 毫秒值，不存在时返回 -1
   */
  public long getCurrentRtt(String ip) {
    return getRtt(ip);
  }

  /**
   * 获取指定 Peer 的上一次 RTT
   *
   * @param ip Peer IP 地址
   * @return previous 毫秒值，不存在时返回 -1
   */
  public long getPreviousRtt(String ip) {
    RttPair pair = table.get(ip);
    return pair != null ? pair.previous : -1L;
  }

  /**
   * 获取全量 RTT 快照
   *
   * @return IP → RttPair 映射的不可变副本
   */
  public Map<String, RttPair> getRttTable() {
    return new HashMap<>(table);
  }

  /**
   * 清空所有 RTT 记录
   */
  public void clear() {
    table.clear();
  }
}
