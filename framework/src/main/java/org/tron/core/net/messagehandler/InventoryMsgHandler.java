package org.tron.core.net.messagehandler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.config.args.Args;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.message.adv.InventoryMessage;
import org.tron.core.net.peer.Item;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.service.adv.AdvService;
import org.tron.protos.Protocol.Inventory.InventoryType;

@Slf4j(topic = "net")
@Component
public class InventoryMsgHandler implements TronMsgHandler {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Autowired
  private AdvService advService;

  @Autowired
  private TransactionsMsgHandler transactionsMsgHandler;

  @Override
  public void processMessage(PeerConnection peer, TronMessage msg) {
    InventoryMessage inventoryMessage = (InventoryMessage) msg;
    InventoryType type = inventoryMessage.getInventoryType();

    // 只记录交易类型，避免区块 INV 干扰统计
    if (type.equals(InventoryType.TRX)) {
      try {
        java.util.Map<String, Object> logEntry = new java.util.HashMap<>();
        logEntry.put("t", System.currentTimeMillis()); // 时间戳
        logEntry.put("ip", peer.getInetAddress().getHostAddress()); // 纯 IP 地址
        logEntry.put("lat", peer.getChannel().getLatestLatency()); // 网络延迟 (ms)
        logEntry.put("sz", inventoryMessage.getHashList().size()); // Hash 数量

        // 记录原始 Hash 列表，方便后续按 Hash 分类统计
        logEntry.put("ids", inventoryMessage.getHashList().stream()
            .map(h -> h.toString())
            .collect(java.util.stream.Collectors.toList()));

        // 使用 [RAW_INV] 标签，方便 grep
        logger.info("[RAW_INV] {}", com.alibaba.fastjson.JSON.toJSONString(logEntry));
      } catch (Exception e) {
        // 记录异常但不要影响主逻辑执行
        logger.error("Error logging inventory data", e);
      }
    }
    if (!check(peer, inventoryMessage)) {
      return;
    }

    for (Sha256Hash id : inventoryMessage.getHashList()) {
      Item item = new Item(id, type);
      peer.getAdvInvReceive().put(item, System.currentTimeMillis());
      advService.addInv(item);
      if (type.equals(InventoryType.BLOCK) && peer.getAdvInvSpread().getIfPresent(item) == null) {
        peer.setLastInteractiveTime(System.currentTimeMillis());
      }
    }
  }

  private boolean check(PeerConnection peer, InventoryMessage inventoryMessage) {

    InventoryType type = inventoryMessage.getInventoryType();
    int size = inventoryMessage.getHashList().size();

    if (peer.isNeedSyncFromPeer() || peer.isNeedSyncFromUs()) {
      logger.warn("Drop inv: {} size: {} from Peer {}, syncFromUs: {}, syncFromPeer: {}",
          type, size, peer.getInetAddress(), peer.isNeedSyncFromUs(), peer.isNeedSyncFromPeer());
      return false;
    }

    if (type.equals(InventoryType.TRX) && tronNetDelegate.isBlockUnsolidified()) {
      logger.warn("Drop inv: {} size: {} from Peer {}, block unsolidified",
          type, size, peer.getInetAddress());
      return false;
    }

    if (type.equals(InventoryType.TRX) && transactionsMsgHandler.isBusy()) {
      logger.warn("Drop inv: {} size: {} from Peer {}, transactionsMsgHandler is busy",
          type, size, peer.getInetAddress());
      if (Args.getInstance().isOpenPrintLog()) {
        logger.warn("[isBusy]Drop tx list is: {}", inventoryMessage.getHashList());
      }
      return false;
    }

    return true;
  }
}
