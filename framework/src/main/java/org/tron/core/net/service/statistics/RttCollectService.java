package org.tron.core.net.service.statistics;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.tron.common.es.ExecutorServiceManager;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.peer.PeerManager;

/**
 * RTT 定时采集服务。
 * 以固定周期遍历所有在线 Peer，读取其平均 RTT 并写入 PeerRttTable，
 * 同时将最新快照输出到 CSV 文件。
 */
@Slf4j(topic = "net")
@Component
public class RttCollectService {

  private final PeerRttTable peerRttTable = new PeerRttTable();
  private final String esName = "rtt-collector";

  /** CSV 输出路径（相对于进程工作目录），可按需修改 */
  private static final String OUTPUT_FILE = "logs/rtt_table.csv";

  private ScheduledExecutorService executor =
      ExecutorServiceManager.newSingleThreadScheduledExecutor(esName);

  /**
   * 启动定时采集，默认每 10 秒全量刷新一次 RTT 数据并写文件
   */
  public void init() {
    executor.scheduleWithFixedDelay(() -> {
      try {
        collect();
        writeToFile();
      } catch (Throwable t) {
        logger.error("Exception in rtt collector worker, {}", t.getMessage());
      }
    }, 10, 10, TimeUnit.SECONDS);
  }

  /**
   * 关闭采集线程
   */
  public void close() {
    ExecutorServiceManager.shutdownAndAwaitTermination(executor, esName);
  }

  /**
   * 对外暴露 RTT 数据表，供业务线程查询
   */
  public PeerRttTable getPeerRttTable() {
    return peerRttTable;
  }

  /**
   * 遍历在线 Peer，将每个 Peer 的平均 RTT 写入表中
   */
  private void collect() {
    for (PeerConnection peer : PeerManager.getPeers()) {
      try {
        String ip = peer.getInetAddress().getHostAddress();
        long rtt = peer.getChannel().getAvgLatency();
        peerRttTable.put(ip, rtt);
      } catch (Exception e) {
        logger.warn("Collect rtt for peer failed, {}", e.getMessage());
      }
    }
  }

  /**
   * 将当前 RTT 表覆写到 CSV 文件，格式：IP,CurrentRTT(ms),PreviousRTT(ms),Time
   */
  private void writeToFile() {
    Map<String, PeerRttTable.RttPair> table = peerRttTable.getRttTable();
    if (table.isEmpty()) {
      return;
    }
    String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    try (PrintWriter pw = new PrintWriter(OUTPUT_FILE)) {
      pw.println("IP,CurrentRTT(ms),PreviousRTT(ms),Time");
      for (Map.Entry<String, PeerRttTable.RttPair> entry : table.entrySet()) {
        PeerRttTable.RttPair pair = entry.getValue();
        pw.printf("%s,%d,%d,%s%n", entry.getKey(), pair.current, pair.previous, now);
      }
    } catch (Exception e) {
      logger.warn("Write rtt table to file failed, {}", e.getMessage());
    }
  }
}
