package com.lorin.hadoop.recommendation.rescorer;

import java.util.Set;

import org.apache.mahout.cf.taste.recommender.IDRescorer;

public class JobRescorer implements IDRescorer {
    final private Set<Long> jobids;

    public JobRescorer(Set<Long> jobs) {
        this.jobids = jobs;
    }

    public double rescore(long id, double originalScore) {
        return isFiltered(id) ? Double.NaN : originalScore;
    }

    public boolean isFiltered(long id) {
        return jobids.contains(id);
    }
}
