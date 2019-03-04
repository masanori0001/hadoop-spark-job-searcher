package com.dena.hadoopLogMon.mrhist;

import java.util.List;
import java.util.HashMap;
import java.util.Iterator;

public class ResultHost {

    public String hostname;
    public int tasks = 0;
    public HashMap<String, ResultTaskInfo> taskMap = new HashMap<String, ResultTaskInfo>();

    public ResultHost(String hostname){
        this.hostname = hostname;
    }

    public void addTask(ResultTaskInfo rti){
        tasks++;
        String taskId = rti.getTaskId();
        taskMap.put(taskId, rti);
    }

    public void addTaskFinished(Task task){
        ResultTaskInfo rti = (ResultTaskInfo) taskMap.get(task.taskId);
        rti.addEndTime(task.time);
    }

    public int getTaskNum(){ return tasks; }

    public String getHostname(){ return hostname; }

    public void output(){
        System.out.println("     hostname : " + hostname + "   task num : " + tasks);
        Iterator it = taskMap.keySet().iterator();
        while(it.hasNext()){
            ResultTaskInfo rti = taskMap.get((String)it.next());
            rti.output();
        }
    }

    public ResultHost filter(Conf conf){
        ResultHost rh = hostFilter(conf);
        if(rh == null) return null;

        return detailFilter(conf);
    }

    private ResultHost hostFilter(Conf conf){
        List<String> filterHosts = conf.getHosts();
        if(filterHosts.size() == 0) return this;
        for(String host: filterHosts){
            if(host.equals(hostname)) return this;
        }

        return null;
    }

    private ResultHost detailFilter(Conf conf){
        if(!conf.getDetail()){
            taskMap.clear();
        }

        return this;
    }

}
