package com.jagame.event.flow;

import com.jagame.event.signal.Signal;

import java.util.Map;

public interface StartFlowSignal extends Signal {

    String endpoint();
    Map<String, String> arguments();

}
