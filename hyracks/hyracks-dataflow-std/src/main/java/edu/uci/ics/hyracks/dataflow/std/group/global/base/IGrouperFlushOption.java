package edu.uci.ics.hyracks.dataflow.std.group.global.base;

public interface IGrouperFlushOption {

    public enum GroupOutputState{
        RAW_STATE, GROUP_STATE, RESULT_STATE
    }
    
    GroupOutputState getOutputState();
    
}
