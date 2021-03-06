/**
 *
 */
package com.prady.sample.tx.response;

/**
 * @author Prady
 *
 */
public class ErrorResponse {

    public enum ErrorCode {
        NOT_FOUND, GENERAL, VALIDATION, ALREADY_EXISTS, INSUFFICIENT_RESOURCES
    }

    private ErrorCode code;
    private String messages;

    /**
     * @param code
     * @param messages
     */
    public ErrorResponse(ErrorCode code, String messages) {
        super();
        this.code = code;
        this.messages = messages;
    }

    /**
     * @return the code
     */
    public ErrorCode getCode() {
        return code;
    }

    /**
     * @return the messages
     */
    public String getMessages() {
        return messages;
    }
}
