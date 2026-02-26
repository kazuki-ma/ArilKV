-------------------------- MODULE ClusterControlPlaneGossipProgress --------------------------
EXTENDS Naturals

CONSTANTS MaxSteps, StartupDelayChoices, FallbackRoundOnShutdown, FallbackOnlyWhenEmpty

Lifecycles == {"STARTUP", "RUNNING", "SHUTDOWN"}

VARIABLES lifecycle, startupRemaining, shutdownPending, gossipCount, redundantFallback, stepsRemaining

Vars == <<lifecycle, startupRemaining, shutdownPending, gossipCount, redundantFallback, stepsRemaining>>

TypeOK ==
    /\ lifecycle \in Lifecycles
    /\ startupRemaining \in StartupDelayChoices
    /\ shutdownPending \in BOOLEAN
    /\ gossipCount \in Nat
    /\ redundantFallback \in BOOLEAN
    /\ stepsRemaining \in 0..MaxSteps
    /\ FallbackRoundOnShutdown \in BOOLEAN
    /\ FallbackOnlyWhenEmpty \in BOOLEAN

Init ==
    /\ lifecycle = "STARTUP"
    /\ startupRemaining \in StartupDelayChoices
    /\ shutdownPending = FALSE
    /\ gossipCount = 0
    /\ redundantFallback = FALSE
    /\ stepsRemaining = MaxSteps

PublishShutdown ==
    /\ stepsRemaining > 0
    /\ lifecycle # "SHUTDOWN"
    /\ ~shutdownPending
    /\ shutdownPending' = TRUE
    /\ UNCHANGED <<lifecycle, startupRemaining, gossipCount, redundantFallback>>
    /\ stepsRemaining' = stepsRemaining - 1

StartupTick ==
    /\ stepsRemaining > 0
    /\ lifecycle = "STARTUP"
    /\ startupRemaining > 0
    /\ startupRemaining' = startupRemaining - 1
    /\ UNCHANGED <<lifecycle, shutdownPending, gossipCount, redundantFallback>>
    /\ stepsRemaining' = stepsRemaining - 1

EnterRunning ==
    /\ stepsRemaining > 0
    /\ lifecycle = "STARTUP"
    /\ startupRemaining = 0
    /\ lifecycle' = "RUNNING"
    /\ startupRemaining' = startupRemaining
    /\ shutdownPending' = shutdownPending
    /\ gossipCount' = gossipCount
    /\ redundantFallback' = redundantFallback
    /\ stepsRemaining' = stepsRemaining - 1

RunGossipRound ==
    /\ stepsRemaining > 0
    /\ lifecycle = "RUNNING"
    /\ ~shutdownPending
    /\ lifecycle' = "RUNNING"
    /\ gossipCount' = gossipCount + 1
    /\ UNCHANGED <<startupRemaining, shutdownPending, redundantFallback>>
    /\ stepsRemaining' = stepsRemaining - 1

ObserveShutdown ==
    /\ stepsRemaining > 0
    /\ lifecycle = "RUNNING"
    /\ shutdownPending
    /\ lifecycle' = "SHUTDOWN"
    /\ startupRemaining' = startupRemaining
    /\ shutdownPending' = shutdownPending
    /\ gossipCount' =
        IF FallbackRoundOnShutdown /\ ((~FallbackOnlyWhenEmpty) \/ (gossipCount = 0))
            THEN gossipCount + 1
            ELSE gossipCount
    /\ redundantFallback' =
        IF FallbackRoundOnShutdown /\ ((~FallbackOnlyWhenEmpty) \/ (gossipCount = 0))
            THEN redundantFallback \/ (gossipCount > 0)
            ELSE redundantFallback
    /\ stepsRemaining' = stepsRemaining - 1

DoneStutter ==
    /\ stepsRemaining = 0 \/ lifecycle = "SHUTDOWN"
    /\ UNCHANGED Vars

Next ==
    \/ PublishShutdown
    \/ StartupTick
    \/ EnterRunning
    \/ RunGossipRound
    \/ ObserveShutdown
    \/ DoneStutter

ShutdownHasAtLeastOneGossip ==
    lifecycle = "SHUTDOWN" => gossipCount > 0

NoRedundantFallback ==
    ~redundantFallback

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

================================================================================
