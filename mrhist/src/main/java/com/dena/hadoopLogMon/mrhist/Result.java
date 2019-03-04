package com.dena.hadoopLogMon.mrhist;

import java.text.SimpleDateFormat;
import java.util.*;

public class Result {

    public List<ResultRow> results = new LinkedList<ResultRow>();

    public void addResult(ResultRow result){
        results.add(result);
    }

    public void output(){
        Iterator it = results.iterator();
        while(it.hasNext()){
            ((ResultRow)it.next()).output();
            System.out.println("");
        }
    }

    public Result filter(Conf conf){
        Result filterResult = new Result();
        Iterator it = results.iterator();
        while(it.hasNext()) {
            ResultRow row = ((ResultRow) it.next()).filter(conf);
            if (row != null) {
                filterResult.addResult(row);
            }
        }

        return filterResult;
    }

    public void count(HashMap<String, Integer> taskCounts){
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy/MM/dd HH");
        results.stream().forEach(v -> {
            Date jobTime = v.getJob().getStart();
            calendar.setTime(jobTime);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            Date hour = calendar.getTime();
            String key = sdFormat.format(hour);
            if(taskCounts.containsKey(key)){
                taskCounts.put(key, taskCounts.get(key) + 1);
            }else{
                taskCounts.put(key, 1);
            }
        });
    }


}
