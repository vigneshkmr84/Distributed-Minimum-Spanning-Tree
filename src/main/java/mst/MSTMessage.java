package mst;

import java.io.Serializable;
import java.util.List;

enum MSTMessageType {
    BROADCAST_TESTING,
    TESTING_NEIGHBORS,
    TEST_MESSAGE_REPLY,
    CONVERGECAST_LEADER,
    BROADCAST_NEW_LEADER,
    MERGE_COMPONENT,
    BROADCAST_COMPLETION,
    UPDATE_ROUND
}

public class MSTMessage implements Serializable {

    int round;

    int uid;

    // leader of that component can be used as component id
    int componentId;

    // search-reply, test, leader //
    // leader: is used when a new leader is elected to broadcast.
    // search: used by leader to check for neighbouring components.
    // reply: used as reply for search message.
    MSTMessageType messageType;

    List<Integer> maxWeight = null;

    public MSTMessage(int round, int uid, int componentId, MSTMessageType messageType) {
        this.round = round;
        this.uid = uid;
        this.componentId = componentId;
        this.messageType = messageType;
    }

    public MSTMessage(int round, int uid, int componentId, MSTMessageType messageType, List<Integer> maxWeight) {
        this.round = round;
        this.uid = uid;
        this.componentId = componentId;
        this.messageType = messageType;
        this.maxWeight = maxWeight;
    }
}
