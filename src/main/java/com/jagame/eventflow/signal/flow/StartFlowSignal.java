package com.jagame.eventflow.signal.flow;

import com.jagame.eventflow.signal.Signal;

import java.util.Map;

public interface StartFlowSignal extends Signal {

    String endpoint();
    Map<String, String> arguments();

}
