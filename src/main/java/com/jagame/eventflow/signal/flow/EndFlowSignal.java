package com.jagame.eventflow.signal.flow;

import com.jagame.eventflow.signal.Signal;

public interface EndFlowSignal extends Signal {

    SignalFlowStatus status();

}
