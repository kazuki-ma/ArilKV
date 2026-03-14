------------------------------ MODULE WaitAckProgress ------------------------------
EXTENDS Naturals, FiniteSets

\* Models WAIT quorum/timeout progression with monotonic downstream ACK offsets.
\* Linked runtime path:
\* - request_lifecycle/server_commands::handle_wait effect emission
\* - connection_handler::materialize_wait_response_if_needed
\* - redis_replication::wait_for_replicas / REPLCONF ACK ledger

CONSTANTS Replicas, MaxOffset, TimeoutBound, RequestedReplicas

Phases == {"IDLE", "WAITING", "DONE_QUORUM", "DONE_TIMEOUT"}

VARIABLES phase, targetOffset, ackOffset, timeoutRemaining

Vars == <<phase, targetOffset, ackOffset, timeoutRemaining>>

AckedReplicas == {r \in Replicas : ackOffset[r] >= targetOffset}
AckedCount == Cardinality(AckedReplicas)

TypeOK ==
    /\ phase \in Phases
    /\ targetOffset \in 0..MaxOffset
    /\ ackOffset \in [Replicas -> 0..MaxOffset]
    /\ timeoutRemaining \in 0..TimeoutBound
    /\ RequestedReplicas \in 0..Cardinality(Replicas)

Init ==
    /\ phase = "IDLE"
    /\ targetOffset = 0
    /\ ackOffset = [r \in Replicas |-> 0]
    /\ timeoutRemaining = TimeoutBound

\* TLA+ : WaitStart
WaitStart ==
    /\ phase = "IDLE"
    /\ phase' = "WAITING"
    /\ targetOffset' \in 0..MaxOffset
    /\ timeoutRemaining' = TimeoutBound
    /\ UNCHANGED ackOffset

\* TLA+ : WaitRequestAckRefresh
WaitRequestAckRefresh ==
    /\ phase = "WAITING"
    /\ UNCHANGED Vars

\* TLA+ : ReplicaAckAdvance
ReplicaAckAdvance ==
    /\ phase = "WAITING"
    /\ \E r \in Replicas:
        \E nextOffset \in ackOffset[r]..MaxOffset:
            /\ nextOffset > ackOffset[r]
            /\ ackOffset' = [ackOffset EXCEPT ![r] = nextOffset]
            /\ UNCHANGED <<phase, targetOffset, timeoutRemaining>>

\* TLA+ : WaitObserveAckQuorum
WaitObserveAckQuorum ==
    /\ phase = "WAITING"
    /\ AckedCount >= RequestedReplicas
    /\ phase' = "DONE_QUORUM"
    /\ UNCHANGED <<targetOffset, ackOffset, timeoutRemaining>>

Tick ==
    /\ phase = "WAITING"
    /\ timeoutRemaining > 0
    /\ timeoutRemaining' = timeoutRemaining - 1
    /\ UNCHANGED <<phase, targetOffset, ackOffset>>

\* TLA+ : WaitObserveTimeout
WaitObserveTimeout ==
    /\ phase = "WAITING"
    /\ timeoutRemaining = 0
    /\ AckedCount < RequestedReplicas
    /\ phase' = "DONE_TIMEOUT"
    /\ UNCHANGED <<targetOffset, ackOffset, timeoutRemaining>>

DoneStutter ==
    /\ phase \in {"DONE_QUORUM", "DONE_TIMEOUT"}
    /\ UNCHANGED Vars

Next ==
    \/ WaitStart
    \/ WaitRequestAckRefresh
    \/ ReplicaAckAdvance
    \/ WaitObserveAckQuorum
    \/ Tick
    \/ WaitObserveTimeout
    \/ DoneStutter

InvAckWithinBounds ==
    \A r \in Replicas : ackOffset[r] \in 0..MaxOffset

InvQuorumStateSound ==
    phase = "DONE_QUORUM" => AckedCount >= RequestedReplicas

InvTimeoutStateSound ==
    phase = "DONE_TIMEOUT" => /\ timeoutRemaining = 0
                            /\ AckedCount < RequestedReplicas

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

===================================================================================
