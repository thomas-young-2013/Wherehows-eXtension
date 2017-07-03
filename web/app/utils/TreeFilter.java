package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by thomas young on 6/26/17.
 */
public class TreeFilter {
    public static void main( String[] args ) throws Exception {

        String key = "/home/hadoop/Desktop/codes/grammer/src/main/resources/dataset.json";
        Set<Integer> set = new HashSet<Integer>();
        set.add(8249);
        set.add(8265);
        JSONObject res = filter(key, set);
        System.out.println(res.toString());
    }

    public static JSONObject filter(String key, Set<Integer> set) throws Exception {
        try {
            // String key = "/home/hadoop/Desktop/codes/grammer/src/main/resources/dataset.json";
            String result = getJSONStringFromFile(key);

            /*Set<Integer> set = new HashSet<Integer>();
            set.add(8249);
            set.add(8265);*/

            JSONObject jsonObj = new JSONObject(result);
            JSONArray jsonArray = jsonObj.getJSONArray("children");
            JSONArray newArray = new JSONArray();
            for (int i=0; i<jsonArray.length(); i++) {
                JSONObject jsonObject = new JSONObject();
                copyNode((JSONObject) jsonArray.get(i), jsonObject);
                newArray.put(jsonObject);
                getUserFilesTree((JSONObject) jsonArray.get(i), jsonObject, set);
            }

            // result json object
            JSONObject resultNode = new JSONObject();
            resultNode.put("children", newArray);
            System.out.println(resultNode.toString());
            return clear(resultNode);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static JSONObject clear(JSONObject origin) throws Exception {
        JSONObject result = new JSONObject();
        JSONArray jsonArray = origin.getJSONArray("children");
        JSONArray array = new JSONArray();
        for (int i=0; i<jsonArray.length(); i++) {
            JSONObject tmp = (JSONObject) jsonArray.get(i);
            JSONObject res = clearAndSpanTree(tmp);
            if (res != null) {
                array.put(res);
            }
        }
        result.put("children", array);
        // System.out.println(result.toString());
        return result;
    }

    public static JSONObject clearAndSpanTree(JSONObject node) throws Exception {
        JSONObject res;
        if (node.getInt("folder") == 0) {
            res = new JSONObject();
            copyNode(node, res);
            return res;
        } else {
            JSONArray jsonArray = node.getJSONArray("children");
            JSONArray array = new JSONArray();
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject tmp = (JSONObject) jsonArray.get(i);
                JSONObject temp = clearAndSpanTree(tmp);
                if (temp != null) {
                    array.put(temp);
                }
            }
            if (array.length() != 0) {
                res = new JSONObject();
                copyNode(node, res);
                res.put("children", array);
                return res;
            }
            return null;
        }
    }

    public static void copyNode(JSONObject jsonObject1, JSONObject jsonObject2) throws Exception {
        jsonObject2.put("level", jsonObject1.getInt("level"));
        jsonObject2.put("title", jsonObject1.getString("title"));
        jsonObject2.put("path", jsonObject1.getString("path"));
        jsonObject2.put("folder", jsonObject1.getInt("folder"));
        JSONArray newArray = new JSONArray();
        jsonObject2.put("children", newArray);
        if (jsonObject1.has("id")) jsonObject2.put("id", jsonObject1.getInt("id"));
    }

    public static String getJSONStringFromFile(String key) {
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(key));
            String tmp;
            String result = "";
            while ((tmp = br.readLine()) != null) {
                result += tmp;
            }
            br.close();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void getUserFilesTree(JSONObject jsonObject, JSONObject jsonObject2,
                                        Set<Integer> fileIdSet) throws Exception {
        if (jsonObject.getInt("folder") == 1) {
            JSONArray jsonArray = jsonObject.getJSONArray("children");
            for (int i=0; i<jsonArray.length(); i++) {
                JSONObject origin = (JSONObject) jsonArray.get(i);
                if (origin.getInt("folder") == 0) {
                    if (!fileIdSet.contains(origin.getInt("id"))) continue;
                }

                JSONObject jsonObject1 = new JSONObject();
                // copy..
                copyNode(origin, jsonObject1);
                jsonObject2.getJSONArray("children").put(jsonObject1);
                getUserFilesTree(origin, jsonObject1, fileIdSet);
            }

        }
    }
}
