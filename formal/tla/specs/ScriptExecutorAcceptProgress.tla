---------------------------- MODULE ScriptExecutorAcceptProgress ----------------------------
EXTENDS Naturals

CONSTANTS MaxSteps, OffloadScriptingWait

ScriptStates == {"IDLE", "RUNNING", "DONE"}
Conn1States == {"NONE", "WAITING", "DONE"}
Conn2States == {"ABSENT", "PENDING", "ACCEPTED", "KILL_SENT", "DONE"}

VARIABLES scriptState, conn1State, conn2State, killRequested, asyncWorkerBlocked, stepsRemaining

Vars == <<scriptState, conn1State, conn2State, killRequested, asyncWorkerBlocked, stepsRemaining>>

TypeOK ==
    /\ scriptState \in ScriptStates
    /\ conn1State \in Conn1States
    /\ conn2State \in Conn2States
    /\ killRequested \in BOOLEAN
    /\ asyncWorkerBlocked \in BOOLEAN
    /\ stepsRemaining \in 0..MaxSteps
    /\ OffloadScriptingWait \in BOOLEAN

Init ==
    /\ scriptState = "IDLE"
    /\ conn1State = "NONE"
    /\ conn2State = "ABSENT"
    /\ killRequested = FALSE
    /\ asyncWorkerBlocked = FALSE
    /\ stepsRemaining = MaxSteps

BeginScript ==
    /\ stepsRemaining > 0
    /\ conn1State = "NONE"
    /\ scriptState = "IDLE"
    /\ scriptState' = "RUNNING"
    /\ conn1State' = "WAITING"
    /\ conn2State' = conn2State
    /\ killRequested' = killRequested
    /\ asyncWorkerBlocked' = ~OffloadScriptingWait
    /\ stepsRemaining' = stepsRemaining - 1

Conn2Arrives ==
    /\ stepsRemaining > 0
    /\ conn2State = "ABSENT"
    /\ conn2State' = "PENDING"
    /\ UNCHANGED <<scriptState, conn1State, killRequested, asyncWorkerBlocked>>
    /\ stepsRemaining' = stepsRemaining - 1

AcceptConn2 ==
    /\ stepsRemaining > 0
    /\ conn2State = "PENDING"
    /\ ~asyncWorkerBlocked
    /\ conn2State' = "ACCEPTED"
    /\ UNCHANGED <<scriptState, conn1State, killRequested, asyncWorkerBlocked>>
    /\ stepsRemaining' = stepsRemaining - 1

SendKill ==
    /\ stepsRemaining > 0
    /\ conn2State = "ACCEPTED"
    /\ scriptState = "RUNNING"
    /\ conn2State' = "KILL_SENT"
    /\ killRequested' = TRUE
    /\ UNCHANGED <<scriptState, conn1State, asyncWorkerBlocked>>
    /\ stepsRemaining' = stepsRemaining - 1

ScriptHandlesKill ==
    /\ stepsRemaining > 0
    /\ scriptState = "RUNNING"
    /\ killRequested
    /\ scriptState' = "DONE"
    /\ conn1State' = "DONE"
    /\ asyncWorkerBlocked' = FALSE
    /\ UNCHANGED <<conn2State, killRequested>>
    /\ stepsRemaining' = stepsRemaining - 1

Conn2Completes ==
    /\ stepsRemaining > 0
    /\ conn2State = "KILL_SENT"
    /\ scriptState = "DONE"
    /\ conn2State' = "DONE"
    /\ UNCHANGED <<scriptState, conn1State, killRequested, asyncWorkerBlocked>>
    /\ stepsRemaining' = stepsRemaining - 1

DoneStutter ==
    /\ stepsRemaining = 0
    /\ UNCHANGED Vars

Next ==
    \/ BeginScript
    \/ Conn2Arrives
    \/ AcceptConn2
    \/ SendKill
    \/ ScriptHandlesKill
    \/ Conn2Completes
    \/ DoneStutter

DeadlockPatternAbsent ==
    ~( /\ scriptState = "RUNNING"
       /\ conn2State = "PENDING"
       /\ killRequested = FALSE
       /\ asyncWorkerBlocked)

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

================================================================================
