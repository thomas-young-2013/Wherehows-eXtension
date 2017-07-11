package utils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import play.libs.Json;

/**
 * Created by thomas young on 7/11/17.
 */
public class ObjectNodeMaker {
    public static ObjectNode getFailedMsg(String msg) {
        ObjectNode node = Json.newObject();
        node.put("status", "failed");
        node.put("msg", msg);
        return node;
    }
}
