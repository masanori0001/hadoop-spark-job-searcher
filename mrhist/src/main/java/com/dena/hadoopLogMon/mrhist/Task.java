package com.dena.hadoopLogMon.mrhist;

import java.util.Date;

public class Task {
    public String taskId;
    public Date time;
    public String hostname;

    public Task(String taskId, Date time){
        this.taskId = taskId;
        this.time = time;
    }

    public Task(String taskId, Date time, String hostname){
        this.taskId = taskId;
        this.time = time;
        this.hostname = hostname;
    }
}
