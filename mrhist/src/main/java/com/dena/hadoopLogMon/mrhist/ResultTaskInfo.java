package com.dena.hadoopLogMon.mrhist;

import java.util.Date;

public class ResultTaskInfo {
    public String taskId;
    public Date start;
    public Date end;

    public void addTaskId(String taskId){
        this.taskId = taskId;
    }

    public void addStartTime(Date start){
        this.start = start;
    }

    public void addEndTime(Date end){
        this.end = end;
    }

    public String getTaskId(){
        return taskId;
    }

    public Date getStartTime(){
        return start;
    }

    public Date getEndTime(){
        return end;
    }

    public void output(){
        System.out.println("         task id : " + taskId + "   start time : " + start + "   finish time: " + end);
    }
}
