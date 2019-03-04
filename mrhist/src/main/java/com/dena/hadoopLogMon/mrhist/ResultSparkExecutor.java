package com.dena.hadoopLogMon.mrhist;

import java.util.List;
import java.util.Iterator;

public class ResultSparkExecutor {

    public String hostname;
    public int execnum;

    public void addHostname(String hostname){
        this.hostname = hostname;
    }

    public void addExecNum(int execnum){
        this.execnum = execnum;
    }

    public String getHostname(){
        return hostname;
    }

    public int getExecnum(){
        return execnum;
    }

    public ResultSparkExecutor filter(Conf conf){
        List<String> hosts = conf.getHosts();
        if(hosts.size() == 0) return this;
        Iterator it = hosts.iterator();
        while(it.hasNext()){
            if(((String)it.next()).equals(hostname)) return this;
        }

        return null;
    }

    public void output(){
        System.out.println("   host: " + hostname + "   executor num: " + execnum);
    }
}
