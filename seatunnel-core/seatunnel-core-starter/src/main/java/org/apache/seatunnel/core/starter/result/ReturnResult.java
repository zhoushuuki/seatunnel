package org.apache.seatunnel.core.starter.result;

import lombok.Data;

/**
 * ClassName: org.apache.seatunnel.core.starter.seatunnel.result.ReturnResult
 * Name:ReturnResult
 * Author: xiong-feng
 * Date: 2023/7/5 9:05
 * Description:
 */
@Data
public class ReturnResult {

    private String startTime;
    private String endTime;
    private Long totalTime;
    private Long totalCount = 0L;
    private Long writeCount = 0L;
    private Long failedCount = 0L;

    private String errorInfo;

    public ReturnResult() {
    }

    public ReturnResult(String startTime, String endTime, Long totalTime, Long totalCount, Long writeCount, Long failedCount) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.totalTime = totalTime;
        this.totalCount = totalCount;
        this.writeCount = writeCount;
        this.failedCount = failedCount;
    }

}
