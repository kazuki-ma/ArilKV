---------------------------- MODULE SwapDbWatchVisibility ----------------------------
EXTENDS Naturals, TLC

CONSTANTS DBS, SLOTS, Values

VARIABLES binding, storageValue, version, watched, watchedBinding, watchedValue,
          watchedVersion, execChecked, execAllowed

WatchedDb == "db1"
OtherDb == CHOOSE db \in DBS : db # WatchedDb
MaxVersion == 2

VisibleValue(db) == storageValue[binding[db]]

Init ==
    /\ binding = [db \in DBS |-> IF db = "db0" THEN "slot0" ELSE "slot1"]
    /\ storageValue \in [SLOTS -> Values]
    /\ version = [db \in DBS |-> 0]
    /\ watched = FALSE
    /\ watchedBinding = "slot1"
    /\ watchedValue = "missing"
    /\ watchedVersion = 0
    /\ execChecked = FALSE
    /\ execAllowed = FALSE

\* TLA+ : CaptureWatchedVisibleState
CaptureWatchedVisibleState ==
    /\ ~watched
    /\ ~execChecked
    /\ watched' = TRUE
    /\ watchedBinding' = binding[WatchedDb]
    /\ watchedValue' = VisibleValue(WatchedDb)
    /\ watchedVersion' = version[WatchedDb]
    /\ execChecked' = FALSE
    /\ execAllowed' = FALSE
    /\ UNCHANGED <<binding, storageValue, version>>

MutateVisibleValue ==
    /\ watched
    /\ ~execChecked
    /\ \E newValue \in Values:
        /\ newValue # VisibleValue(WatchedDb)
        /\ storageValue' = [storageValue EXCEPT ![binding[WatchedDb]] = newValue]
    /\ version' =
        [version EXCEPT ![WatchedDb] = IF @ < MaxVersion THEN @ + 1 ELSE MaxVersion]
    /\ UNCHANGED <<binding, watched, watchedBinding, watchedValue, watchedVersion,
                   execChecked, execAllowed>>

\* TLA+ : InvalidateSwapTrackedKeys
InvalidateSwapTrackedKeys ==
    /\ watched
    /\ ~execChecked
    /\ UNCHANGED <<binding, storageValue, version, watched, watchedBinding, watchedValue,
                   watchedVersion, execChecked, execAllowed>>

\* TLA+ : SwapLogicalDbBindings
SwapLogicalDbBindings ==
    /\ watched
    /\ ~execChecked
    /\ binding' = [binding EXCEPT ![WatchedDb] = binding[OtherDb], ![OtherDb] = binding[WatchedDb]]
    /\ UNCHANGED <<storageValue, version, watched, watchedBinding, watchedValue,
                   watchedVersion, execChecked, execAllowed>>

\* TLA+ : ValidateWatchedVisibleState
ValidateWatchedVisibleState ==
    /\ watched
    /\ ~execChecked
    /\ execChecked' = TRUE
    /\ execAllowed' =
        IF binding[WatchedDb] = watchedBinding
            THEN version[WatchedDb] = watchedVersion
            ELSE VisibleValue(WatchedDb) = watchedValue
    /\ UNCHANGED <<binding, storageValue, version, watched, watchedBinding, watchedValue,
                   watchedVersion>>

StutterAfterExecDecision ==
    /\ execChecked
    /\ UNCHANGED <<binding, storageValue, version, watched, watchedBinding, watchedValue,
                   watchedVersion, execChecked, execAllowed>>

Next ==
    \/ CaptureWatchedVisibleState
    \/ MutateVisibleValue
    \/ InvalidateSwapTrackedKeys
    \/ SwapLogicalDbBindings
    \/ ValidateWatchedVisibleState
    \/ StutterAfterExecDecision

ExecDecisionCorrect ==
    execChecked =>
        execAllowed =
            (IF binding[WatchedDb] = watchedBinding
                THEN version[WatchedDb] = watchedVersion
                ELSE VisibleValue(WatchedDb) = watchedValue)

SwapNoFalseAbortForSameVisibleValue ==
    execChecked /\ binding[WatchedDb] # watchedBinding /\ VisibleValue(WatchedDb) = watchedValue
        => execAllowed

SwapAbortForChangedVisibleValue ==
    execChecked /\ binding[WatchedDb] # watchedBinding /\ VisibleValue(WatchedDb) # watchedValue
        => ~execAllowed

Spec == Init /\ [][Next]_<<binding, storageValue, version, watched, watchedBinding,
                          watchedValue, watchedVersion, execChecked, execAllowed>>

=============================================================================
