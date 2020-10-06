package io.dogy.util;

import lombok.Data;

@Data
public class CurrentStatusInfo {

    private String name;
    private long queued;
    private long active;
    private long notCompleted;
    private long inDisk;
    private long waitRetry;
    private long frequency;
    private boolean warning;

}
