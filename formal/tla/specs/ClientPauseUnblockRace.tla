--------------------------- MODULE ClientPauseUnblockRace ---------------------------
EXTENDS Naturals

\* Models the pause->unpause->unblock ordering race for blocking commands:
\* - blocking command reaches CLIENT PAUSE gate
\* - UNPAUSE happens
\* - CLIENT UNBLOCK is requested before the blocking loop is fully resumed
\* - bug variant clears pending unblock on resume and can lose the request

CONSTANTS MaxSteps, InitialTimeout, ResumeClearsPending

Phases == {"ACTIVE", "PAUSE_BLOCKED", "BLOCKED", "DONE_UNBLOCKED", "DONE_TIMEOUT"}

VARIABLES
    phase,
    pauseActive,
    pauseBlockedMarker,
    pendingUnblock,
    acceptedUnblockAfterUnpause,
    timeoutRemaining,
    stepsRemaining

Vars ==
    <<phase, pauseActive, pauseBlockedMarker, pendingUnblock,
      acceptedUnblockAfterUnpause, timeoutRemaining, stepsRemaining>>

TypeOK ==
    /\ phase \in Phases
    /\ pauseActive \in BOOLEAN
    /\ pauseBlockedMarker \in BOOLEAN
    /\ pendingUnblock \in BOOLEAN
    /\ acceptedUnblockAfterUnpause \in BOOLEAN
    /\ timeoutRemaining \in 0..InitialTimeout
    /\ stepsRemaining \in 0..MaxSteps
    /\ InitialTimeout \in 0..MaxSteps
    /\ ResumeClearsPending \in BOOLEAN

Init ==
    /\ phase = "ACTIVE"
    /\ pauseActive = TRUE
    /\ pauseBlockedMarker = FALSE
    /\ pendingUnblock = FALSE
    /\ acceptedUnblockAfterUnpause = FALSE
    /\ timeoutRemaining = InitialTimeout
    /\ stepsRemaining = MaxSteps

\* TLA+ : PauseGateRegisterBlockingClient
PauseGateRegisterBlockingClient ==
    /\ stepsRemaining > 0
    /\ phase = "ACTIVE"
    /\ pauseActive
    /\ phase' = "PAUSE_BLOCKED"
    /\ pauseBlockedMarker' = TRUE
    /\ UNCHANGED <<pauseActive, pendingUnblock, acceptedUnblockAfterUnpause, timeoutRemaining>>
    /\ stepsRemaining' = stepsRemaining - 1

\* TLA+ : ClientUnpause
ClientUnpause ==
    /\ stepsRemaining > 0
    /\ phase \in {"PAUSE_BLOCKED", "BLOCKED"}
    /\ pauseActive
    /\ pauseActive' = FALSE
    /\ UNCHANGED <<phase, pauseBlockedMarker, pendingUnblock,
                  acceptedUnblockAfterUnpause, timeoutRemaining>>
    /\ stepsRemaining' = stepsRemaining - 1

\* TLA+ : ClientUnblockRequest
ClientUnblockRequest ==
    /\ stepsRemaining > 0
    /\ phase \in {"PAUSE_BLOCKED", "BLOCKED"}
    /\ IF pauseActive
          THEN /\ UNCHANGED <<phase, pauseActive, pauseBlockedMarker, pendingUnblock,
                             acceptedUnblockAfterUnpause, timeoutRemaining>>
          ELSE /\ pendingUnblock' = TRUE
               /\ acceptedUnblockAfterUnpause' = TRUE
               /\ UNCHANGED <<phase, pauseActive, pauseBlockedMarker, timeoutRemaining>>
    /\ stepsRemaining' = stepsRemaining - 1

\* TLA+ : ResumeBlockingAfterPause
ResumeBlockingAfterPause ==
    /\ stepsRemaining > 0
    /\ phase = "PAUSE_BLOCKED"
    /\ ~pauseActive
    /\ phase' = "BLOCKED"
    /\ pauseBlockedMarker' = FALSE
    /\ pendingUnblock' = IF ResumeClearsPending THEN FALSE ELSE pendingUnblock
    /\ acceptedUnblockAfterUnpause' = acceptedUnblockAfterUnpause
    /\ UNCHANGED <<pauseActive, timeoutRemaining>>
    /\ stepsRemaining' = stepsRemaining - 1

\* TLA+ : ObservePendingUnblock
ObservePendingUnblock ==
    /\ stepsRemaining > 0
    /\ phase = "BLOCKED"
    /\ pendingUnblock
    /\ phase' = "DONE_UNBLOCKED"
    /\ pendingUnblock' = FALSE
    /\ UNCHANGED <<pauseActive, pauseBlockedMarker, acceptedUnblockAfterUnpause, timeoutRemaining>>
    /\ stepsRemaining' = stepsRemaining - 1

BlockingTimeoutTick ==
    /\ stepsRemaining > 0
    /\ phase = "BLOCKED"
    /\ timeoutRemaining > 0
    /\ timeoutRemaining' = timeoutRemaining - 1
    /\ UNCHANGED <<phase, pauseActive, pauseBlockedMarker, pendingUnblock,
                  acceptedUnblockAfterUnpause>>
    /\ stepsRemaining' = stepsRemaining - 1

\* TLA+ : BlockingTimeout
BlockingTimeout ==
    /\ stepsRemaining > 0
    /\ phase = "BLOCKED"
    /\ timeoutRemaining = 0
    /\ ~pendingUnblock
    /\ phase' = "DONE_TIMEOUT"
    /\ UNCHANGED <<pauseActive, pauseBlockedMarker, pendingUnblock,
                  acceptedUnblockAfterUnpause, timeoutRemaining>>
    /\ stepsRemaining' = stepsRemaining - 1

DoneStutter ==
    /\ phase \in {"DONE_UNBLOCKED", "DONE_TIMEOUT"} \/ stepsRemaining = 0
    /\ UNCHANGED Vars

Next ==
    \/ PauseGateRegisterBlockingClient
    \/ ClientUnpause
    \/ ClientUnblockRequest
    \/ ResumeBlockingAfterPause
    \/ ObservePendingUnblock
    \/ BlockingTimeoutTick
    \/ BlockingTimeout
    \/ DoneStutter

\* If an unblock request is accepted after unpause, we must not finish as timeout.
NoLostUnblockAfterUnpause ==
    ~(phase = "DONE_TIMEOUT" /\ acceptedUnblockAfterUnpause)

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

=====================================================================================
