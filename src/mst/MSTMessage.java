package mst;

import java.io.Serializable;

public enum MSTMessageType {
    BROADCAST_TESTING,
    TESTING_NEIGHBORS,
    CONVERGECAST_LEADER,
    BROADCAST_NEW_LEADER,
    MERGE_COMPONENT,
    BROADCAST_COMPLETION
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

    public MSTMessage(int round, int uid, int componentId, MSTMessageType messageType) {
        this.round = round;
        this.uid = uid;
        this.componentId = componentId;
        this.messageType = messageType;
    }
}
