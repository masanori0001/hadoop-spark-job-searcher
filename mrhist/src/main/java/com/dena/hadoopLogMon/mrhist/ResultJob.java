package com.dena.hadoopLogMon.mrhist;

import java.util.Date;

public class ResultJob {
    public Date start, end;
    public String jobName;
    public String jobId;
    public long duration;

    public void addStartTime(Date start){
        this.start = start;
        if(end != null){
            duration = end.getTime() - start.getTime();
        }
    }

    public void addEndTime(Date end){
        this.end = end;
        if(start != null){
            duration = end.getTime() - start.getTime();
        }
    }

    public void addJobName(String jobName){
        this.jobName = jobName;
    }

    public void addJonId(String jobId){
        this.jobId = jobId;
    }

    public Date getStart(){ return start; }

    public void output(){
        System.out.println("JobName : " + jobName);
        System.out.println("JobID : " + jobId);
        System.out.println("Start Time : " + start);
        System.out.println("End Time   : " + end);
        System.out.println("Duration   : " + duration);
    }
}
