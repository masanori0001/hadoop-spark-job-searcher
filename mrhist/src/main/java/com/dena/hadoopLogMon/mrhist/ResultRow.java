package com.dena.hadoopLogMon.mrhist;

import java.util.*;

public class ResultRow {

    public ResultJob job;

    public HashMap<String, ResultHost> mhosts;
    public HashMap<String, ResultHost> rhosts;
    public int mapnum = 0, reducenum = 0;

    public void addJob(ResultJob job){
        this.job = job;
    }

    public ResultJob getJob(){ return job; }

    public void addMHosts(HashMap<String, ResultHost> mhosts) {
        this.mhosts = mhosts;
        Iterator it = mhosts.keySet().iterator();
        while(it.hasNext()){
            int count = mhosts.get((String)it.next()).getTaskNum();
            mapnum += count;
        }
    }

    public HashMap<String, ResultHost> getMHosts(){ return mhosts; }

    public void addRHosts(HashMap<String, ResultHost> rhosts) {
        this.rhosts = rhosts;
        Iterator it = rhosts.keySet().iterator();
        while(it.hasNext()){
            int count = rhosts.get((String)it.next()).getTaskNum();
            reducenum += count;
        }
    }

    public HashMap<String, ResultHost> getRHosts(){ return rhosts; }

    public void output(){
        job.output();
        System.out.println("  MapHosts:  tasks  " + mapnum);
        Iterator it = mhosts.keySet().iterator();
        while(it.hasNext()) {
            ResultHost host = mhosts.get((String) it.next());
            host.output();
        }

        System.out.println("  ReduceHosts:  tasks  " + reducenum);
        it = rhosts.keySet().iterator();
        while(it.hasNext()) {
            ResultHost host = rhosts.get((String)it.next());
            host.output();
        }

    }

    public ResultRow filter(Conf conf){
        ResultRow filterRow = new ResultRow();
        filterRow.addJob(job);

        HashMap<String, ResultHost> mnew = new HashMap<String, ResultHost>();
        Iterator it = mhosts.keySet().iterator();
        while(it.hasNext()){
            ResultHost host = mhosts.get((String)it.next()).filter(conf);
            if(host != null) {
                mnew.put(host.getHostname(), host);
            }
        }
        filterRow.addMHosts(mnew);

        HashMap<String, ResultHost> rnew = new HashMap<String, ResultHost>();
        it = rhosts.keySet().iterator();
        while(it.hasNext()){
            ResultHost host = rhosts.get((String)it.next()).filter(conf);
            if(host != null) {
                rnew.put(host.getHostname(), host);
            }
        }
        filterRow.addRHosts(rnew);

        if(filterRow.getMHosts().size() == 0 && filterRow.getRHosts().size() == 0) return null;

        return filterRow;
    }

}
