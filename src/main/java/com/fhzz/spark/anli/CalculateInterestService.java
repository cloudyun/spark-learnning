package com.fhzz.spark.anli;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fhzz.util.RedisUtil;

public class CalculateInterestService {
	private String dictKey = "greate_item_sim_2.0";
    private String recommendTable = "great_recommend_table_2.0";
    static final String HIGO_BASE_URL = "jdbc:mysql://*.*.*.*:3212/*";
    static final String HIGO_BASE_USER = "*";
    static final String HIGO_BASE_PASS = "*";

    public LinkedHashMap<Long, Double> calculateInterest(String userId, String traceGoodsId) {
        LinkedHashMap<Long, Double> sortedMap = new LinkedHashMap<Long, Double>();
        String[] simGoods = RedisUtil.getJedis().hget(dictKey, traceGoodsId).split(",");
        //用户的历史记录,应该存action:goodsId:timestamp格式,要重构,bi写入单独的数据表中
        HashMap<Long, String> userTrace = null;
        try {
            userTrace = getUserTrace(userId);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return sortedMap;
        }
        HashMap<Long, Double> recommendMap = new HashMap<Long, Double>();
        String[] simGoodsIds = new String[simGoods.length];
        for (int i = 0; i < simGoods.length; i++) {
            simGoodsIds[i] = simGoods[i].split(":")[0];
        }
        List<String> pSimGoodsIds = RedisUtil.getJedis().hmget(dictKey, simGoodsIds);
        HashMap<Long, String> predictSimGoodsIds = new HashMap<Long, String>();
        for (int i = 0; i < simGoodsIds.length; i++) {
            predictSimGoodsIds.put(Long.parseLong(simGoodsIds[i]), pSimGoodsIds.get(i));
        }
        for (String item : simGoods) {
            //need optimised

            Double totalSum = 0.0;
            Double sum = 0.0;
            Long originGoodsId = Long.parseLong(item.split(":")[0]);
            for (String predictGoods : predictSimGoodsIds.get(originGoodsId).split(",")) {
                Long goodsId = Long.parseLong(predictGoods.split(":")[0].toString());
                Double sim = Double.valueOf(predictGoods.split(":")[1].toString());
                totalSum = totalSum + sim;
                Double score = 0.0;
                if (!userTrace.containsKey(goodsId)) {
                    //TODO 用户评分矩阵过于稀疏,需要svd补充评分,暂时无评分score为默认0.1
                    userTrace.put(goodsId, "default");
                }
                String action = userTrace.get(goodsId);


                if (action.equals("click")) {
                    score = 0.2;
                } else if (action.equals("favorate")) {

                } else if (action.equals("add_cart")) {
                    score = 0.6;
                } else if (action.equals("order")) {
                    score = 0.8;

                } else if (action.equals("default")) {

                    score = 0.1;
                }
                //相似度词典应存 goodsid:sim格式,要重构
                sum = sum + score * sim;
            }

            Double predictResult = sum / totalSum;
            recommendMap.put(originGoodsId, predictResult);
        }

        //sort recommend list
        List<Map.Entry<Long, Double>> list = new ArrayList<Map.Entry<Long, Double>>(recommendMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Long, Double>>() {
            @Override
            public int compare(Map.Entry<Long, Double> o1, Map.Entry<Long, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        Map.Entry<Long, Double> tmpEntry = null;
        Iterator<Map.Entry<Long, Double>> iter = list.iterator();
        while (iter.hasNext()) {
            tmpEntry = iter.next();
            sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
        }

        writeRecommendListToRedis(userId, sortedMap);

        return sortedMap;

    }

    private HashMap<Long, String> getUserTrace(String userId) throws ClassNotFoundException {
        //SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        Class.forName("com.mysql.jdbc.Driver");
        PreparedStatement stmt = null;
        Connection conn = null;
        UserTrace userTrace = new UserTrace();
        try {
            conn = DriverManager.getConnection(HIGO_BASE_URL, HIGO_BASE_USER, HIGO_BASE_PASS);
            String sql = "select * from t_pandora_goods_record where account_id=" + userId;
            stmt = (PreparedStatement)conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            while(rs.next()) {
                userTrace.setId(Long.parseLong(rs.getString(1)));
                userTrace.setAccountId(Long.parseLong(rs.getString(2)));
                userTrace.setGoodsIds(rs.getString(3));
                userTrace.setMtime(rs.getString(4));
            }
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        String[] goodsActionTimestamp = userTrace.getGoodsIds().split(",");
        HashMap<Long, String> hm = new HashMap<Long, String>();
        for (String ac : goodsActionTimestamp) {
            Long goodsId = Long.parseLong(ac.split(":")[0]);
            //String action = ac.split(":")[1];
            //String timestamp = ac.split(":")[2];
            //hack 下一步要bi把用户历史行为写入表中, action:goodsId:timestamp格式, timestamp后期将参与权重计算
            String action = "click";
            hm.put(goodsId, action);
        }
        return hm;
    }

    private void writeRecommendListToRedis(String userId, LinkedHashMap<Long, Double> sortedMap) {
        String recommendList = "";
        int count = 0;
        for (Map.Entry<Long, Double> entry : sortedMap.entrySet()) {
            recommendList = recommendList + entry.getKey();
            if (count == sortedMap.size() - 1) {
                break;
            }
            count = count + 1;
            recommendList = recommendList + ",";
        }
        RedisUtil.getJedis().hset(recommendTable, userId, recommendList);
    }

}
