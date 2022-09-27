package com.dbs.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.*;
import java.util.stream.Collectors;

@Data
public class SqlParserUtil {

    private JSONObject sqlJson = new JSONObject();

    private static final List<String> aggregate = Arrays.asList("sum","min","max","avg","distinct","count");

    private static final List<String> whereFormula = Arrays.asList("=","!=","max","avg","distinct","count");

    public static String sqlToJson(String sql){
        JSONObject sqlJson = getNewJson();
        List<JSONObject> selectList = new ArrayList<>();
        List<JSONObject> andList = new ArrayList<>();
        JSONObject andJson = getNewJson();
        JSONObject whereJson = getNewJson();
        /**
         * select
          */
        int selectIndex = sql.indexOf("select");
        int fromIndex = sql.indexOf("from");
        String selectItemString = sql.substring(selectIndex + "select".length(), fromIndex);
        // distinct,count,agg
        // alias
        String[] selectItems = selectItemString.split(",");
        for (String selectItem : selectItems) {
            JSONObject select = getNewJson();
            String item = selectItem;
            JSONObject column = getNewJson();
            int asIndex = item.indexOf("as ");
            // alias
            if (item.indexOf("as ") >= 0) {
                String alias = item.substring(asIndex + "as ".length())
                        .replaceAll(" ","");
                // set alias
                column.put("alias",alias);
                item = item.substring(0,asIndex).trim();
            }
            // agg
            tag1:for (String func : aggregate) {
                if (selectItem.trim().indexOf(func + "(") >= 0) {
                    int funcIndex = selectItem.indexOf(func);
                    item = selectItem.substring(funcIndex + func.length(), asIndex<0?selectItem.length():asIndex)
                            .replaceAll("\\(","")
                            .replaceAll("\\)","")
                            .trim();
                    // set agg
                    column.put("agg",func);
                }
            }
            /*if(column == null || column.equals(null)){
                selectList.add("");
            }*/
            select.put(item,column);
            selectList.add(select);
        }
        sqlJson.put("select",selectList);

        /**
         * from
          */
        int whereIndex = sql.indexOf("where");
        String from = sql.substring(fromIndex+"from".length(), whereIndex == 0 ? sql.length() : whereIndex).trim();
        sqlJson.put("from",from);

        /**
         * where
         */
        int groupIndex = sql.indexOf("group");
        String whereItemString = sql.substring(whereIndex + "where".length(), groupIndex < 0 ? sql.length() : groupIndex);
        // name = 'jack' and age <4o and not ( age <60 and name like '%ma') and address in ('hz','sz')
        String[] whereItems = whereItemString.split("and");
        String itemA = "";
        for (String whereItem : whereItems) {
            JSONObject and = getNewJson();
            String itemAB = whereItem;
            // found '(' but no ')'
            if (whereItem.indexOf("(") >= 0 && whereItem.indexOf(")") <0) {
                itemA = whereItem;
                continue;
            }
            if(whereItem.indexOf(")") >=0){
                itemAB = itemA + itemAB;
            }
            tag2:for (WhereFormula value : WhereFormula.values()) {
                if (itemAB.indexOf(value.getFormula())>0) {
                    // name = 'jack' age < 40
                    // not (age < 60 and name like '%ma')
                    // address in ('hz','sz')
                    String[] whereItemLR = itemAB.trim().split(value.getFormula());
                    if (value.getFormula().equals("in")) {

                    }
                    JSONObject columnFormula = getNewJson();
                    /**
                     * "column1": {
                     *     "eq":[A]  ????
                     * }
                     */
                    columnFormula.put(value.getLabel(),whereItemLR[1]);
                    and.put(whereItemLR[0],columnFormula);
                    itemAB = "";
                    break tag2;
                }
            }
            itemAB = "";
            andList.add(and);
            continue;
        }
        andJson.put("and",andList);
        whereJson.put("and",andJson);
        sqlJson.put("where",andJson);

        /**
         * group
         */
        if(groupIndex > 0){
            String[] groupItem = sql.trim().substring(groupIndex + "group".length()).split(",");
            sqlJson.put("group",groupItem);
        }
        return JSONObject.toJSONString(sqlJson);
    }

    public static String jsonToSql(String jsonString){
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select ");
        JSONObject sqlJson = JSONObject.parseObject(jsonString);
        /**
         * select
         */
        List<JSONObject> select = sqlJson.getJSONArray("select").toJavaList(JSONObject.class);
        Iterator<JSONObject> iterator = select.iterator();
        while (iterator.hasNext()){
            JSONObject next = iterator.next();
            String selectKey = next.keySet().stream().findFirst().get();
            JSONObject selectItem = (JSONObject) next.get(selectKey);
            if ( selectItem!= null) {

                String agg = (String) selectItem.get("agg");
                if(agg == null){
                    sqlBuilder.append(selectKey);
                }else {
                    sqlBuilder.append(agg).append("(").append(selectKey).append(")");
                }

                String alias = (String) selectItem.get("alias");
                if(alias != null){
                    sqlBuilder.append(" as ").append(alias);
                }
            }
            sqlBuilder.append(",");
            System.out.println(selectKey);
        }
        sqlBuilder.replace(sqlBuilder.lastIndexOf(","),sqlBuilder.lastIndexOf(",")+1,"");
        /**
         * from
         */
        String from = (String) sqlJson.get("from");
        sqlBuilder.append(" ").append(from);
        /**
         * where
         */
        JSONObject where = sqlJson.getJSONObject("where");
        List<JSONObject> and = where.getJSONArray("and").toJavaList(JSONObject.class);
        if(and.size()>0){
            sqlBuilder.append(" where ");

        }
        Iterator<JSONObject> andIte = and.iterator();
        while (andIte.hasNext()){
            JSONObject next = andIte.next();
            String whereKey = next.keySet().stream().findFirst().get();
            JSONObject whereItem = (JSONObject) next.get(whereKey);
            String formula = whereItem.keySet().stream().findFirst().get();
            sqlBuilder.append(whereKey);
            String whereItemCondition = whereItem.getString(formula);
            String formulaByLabel = WhereFormula.getFormulaByLabel(formula);
            sqlBuilder.append(formulaByLabel).append(whereItemCondition);
            sqlBuilder.append(" and");
        }
        sqlBuilder.replace(sqlBuilder.lastIndexOf(" and"),sqlBuilder.lastIndexOf(" and")+1,"");
        System.out.println(sqlBuilder.toString());
        return null;
    }


    private static JSONObject getNewJson(){
        return new JSONObject();
    }

}

enum WhereFormula{
    EQ("=","eq"),
    NE("!=","ne"),
    LIKE("like","like"),
    NLIKE("not like","nlike"),
    IN("in","in"),
    NIN("not in","nin");

    private String formula;
    private String label;
    WhereFormula(String formula,String label){
        this.formula = formula;
        this.label = label;
    }

    public String getFormula() {
        return formula;
    }

    public String getLabel() {
        return label;
    }

    static String getFormulaByLabel(String label){
        for (WhereFormula value : WhereFormula.values()) {
            if(value.label.equals(label)){
                return value.formula;
            }
        }
        return null;
    }

    static String getLabelByFormula(String formula){
        for (WhereFormula value : WhereFormula.values()) {
            if(value.formula.equals(formula)){
                return value.label;
            }
        }
        return null;
    }
}
