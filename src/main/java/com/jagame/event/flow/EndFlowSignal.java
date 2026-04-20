package com.jagame.event.flow;

import com.jagame.event.signal.Signal;

public interface EndFlowSignal extends Signal {

    SignalFlowStatus status();

}
