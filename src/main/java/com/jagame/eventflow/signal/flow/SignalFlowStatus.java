package com.jagame.eventflow.signal.flow;

import java.util.Objects;

public class SignalFlowStatus {

    private final String message;
    private final int code;

    public SignalFlowStatus(String message, int code) {
        this.message = message;
        this.code = code;
    }

    public String message() {
        return message;
    }

    public int code() {
        return code;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SignalFlowStatus that = (SignalFlowStatus) o;
        return code == that.code && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, code);
    }

    @Override
    public String toString() {
        return "FlowStatus{" +
                "message='" + message + '\'' +
                ", code=" + code +
                '}';
    }
}
