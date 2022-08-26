package com.github.cysong.dbassert.report;

/**
 * report interface
 *
 * @author cysong
 * @date 2022/8/26 10:39
 **/
public interface Reporter {

    /**
     * start a step
     *
     * @param name
     * @author cysong
     * @date 2022/8/26 10:46
     **/
    void startStep(String name);

    /**
     * end a step
     *
     * @param status step status
     * @author cysong
     * @date 2022/8/26 10:46
     **/
    void endStep(Status status);

    /**
     * end a step with a Throwable
     *
     * @param throwable throwable throw by current step
     * @author cysong
     * @date 2022/8/26 12:00
     **/
    void endStep(Throwable throwable);

    /**
     * add a attachment to current step
     *
     * @param name    attachment name
     * @param content attachment content
     * @author cysong
     * @date 2022/8/26 10:49
     **/
    void addAttachment(String name, String content);

    /**
     * add a attachment to current step
     *
     * @param name    attachment name
     * @param type    attachment type(http content type)
     * @param content attachment content
     * @author cysong
     * @date 2022/8/26 10:49
     **/
    void addAttachment(String name, String type, String content, String extension);
}
