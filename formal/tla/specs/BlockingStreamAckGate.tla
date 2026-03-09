---------------------------- MODULE BlockingStreamAckGate ----------------------------
EXTENDS Naturals

CONSTANTS Pivot, MaxStreamId, UsePredicateReady

VARIABLES streamId, waiterState, writerState

Vars == <<streamId, waiterState, writerState>>

WaiterTracked == waiterState = "BLOCKED"
WaiterReady == waiterState = "BLOCKED" /\ streamId > Pivot
BugAckBlocked == waiterState = "BLOCKED" /\ streamId > 0
AckBlocked == IF UsePredicateReady THEN WaiterReady ELSE BugAckBlocked

TypeOK ==
    /\ streamId \in 0..MaxStreamId
    /\ waiterState \in {"UNTRACKED", "BLOCKED", "DELIVERED"}
    /\ writerState \in {"CAN_WRITE", "AWAIT_ACK", "DONE"}

Init ==
    /\ streamId = 0
    /\ waiterState = "UNTRACKED"
    /\ writerState = "CAN_WRITE"

TrackWaitPredicate ==
    /\ waiterState = "UNTRACKED"
    /\ waiterState' = "BLOCKED"
    /\ UNCHANGED <<streamId, writerState>>

WriteEntry ==
    /\ WaiterTracked
    /\ writerState = "CAN_WRITE"
    /\ streamId < MaxStreamId
    /\ streamId' = streamId + 1
    /\ writerState' = "AWAIT_ACK"
    /\ UNCHANGED waiterState

ProducerAckWaitReady ==
    /\ writerState = "AWAIT_ACK"
    /\ ~AckBlocked
    /\ writerState' = IF streamId = MaxStreamId THEN "DONE" ELSE "CAN_WRITE"
    /\ UNCHANGED <<streamId, waiterState>>

WakeReadyWaiter ==
    /\ WaiterReady
    /\ waiterState' = "DELIVERED"
    /\ UNCHANGED <<streamId, writerState>>

Completed ==
    /\ waiterState = "DELIVERED"
    /\ writerState = "DONE"
    /\ UNCHANGED Vars

Next ==
    \/ TrackWaitPredicate
    \/ WriteEntry
    \/ ProducerAckWaitReady
    \/ WakeReadyWaiter
    \/ Completed

InvNonReadyWaiterDoesNotBlockAck ==
    /\ writerState = "AWAIT_ACK"
    /\ waiterState = "BLOCKED"
    /\ streamId > 0
    /\ streamId <= Pivot
    => ~AckBlocked

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

======================================================================================
