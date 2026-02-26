-------------------------- MODULE LazyExpireReplicationSyncSelectRace --------------------------
EXTENDS Naturals, Sequences

\* Model target:
\* `sync_replication_stream_propagates_lazy_expire_del_from_get_without_replicating_get`
\* Required post-SYNC replication order for the test path: SELECT -> DEL(foo) -> SET(x,1).
\*
\* The race captured here is whether SELECT re-arming happens at SYNC subscribe-time
\* (safe) or only after switching to the replica streaming loop (racy).

CONSTANTS MaxSteps, ArmSelectOnSubscribe

Phases == {"INIT", "SUBSCRIBED", "STREAMING"}
KeyStates == {"LIVE", "DELETED"}
FrameValues == {"SELECT", "DEL", "SET"}

VARIABLES phase, selectNeeded, keyState, lazyDelPublished, setPublished, postSyncFrames, stepsRemaining

Vars == <<phase, selectNeeded, keyState, lazyDelPublished, setPublished, postSyncFrames, stepsRemaining>>

TypeOK ==
    /\ phase \in Phases
    /\ selectNeeded \in BOOLEAN
    /\ keyState \in KeyStates
    /\ lazyDelPublished \in BOOLEAN
    /\ setPublished \in BOOLEAN
    /\ postSyncFrames \in Seq(FrameValues)
    /\ stepsRemaining \in 0..MaxSteps
    /\ ArmSelectOnSubscribe \in BOOLEAN

Init ==
    /\ phase = "INIT"
    /\ selectNeeded = FALSE
    /\ keyState = "LIVE"
    /\ lazyDelPublished = FALSE
    /\ setPublished = FALSE
    /\ postSyncFrames = <<>>
    /\ stepsRemaining = MaxSteps

SyncSubscribe ==
    /\ stepsRemaining > 0
    /\ phase = "INIT"
    /\ phase' = "SUBSCRIBED"
    /\ selectNeeded' =
        IF ArmSelectOnSubscribe
            THEN TRUE
            ELSE selectNeeded
    /\ UNCHANGED <<keyState, lazyDelPublished, setPublished, postSyncFrames>>
    /\ stepsRemaining' = stepsRemaining - 1

EnterStreamingLoop ==
    /\ stepsRemaining > 0
    /\ phase = "SUBSCRIBED"
    /\ phase' = "STREAMING"
    /\ selectNeeded' =
        IF ArmSelectOnSubscribe
            THEN selectNeeded
            ELSE TRUE
    /\ UNCHANGED <<keyState, lazyDelPublished, setPublished, postSyncFrames>>
    /\ stepsRemaining' = stepsRemaining - 1

\* GET observed before expiration: no replication frame and key stays live.
ObserveLiveGet ==
    /\ stepsRemaining > 0
    /\ phase \in {"SUBSCRIBED", "STREAMING"}
    /\ keyState = "LIVE"
    /\ UNCHANGED <<phase, selectNeeded, keyState, lazyDelPublished, setPublished, postSyncFrames>>
    /\ stepsRemaining' = stepsRemaining - 1

\* GET observes expiration and triggers synthetic DEL propagation.
GetExpiresAndReplicatesDel ==
    /\ stepsRemaining > 0
    /\ phase \in {"SUBSCRIBED", "STREAMING"}
    /\ keyState = "LIVE"
    /\ keyState' = "DELETED"
    /\ lazyDelPublished' = TRUE
    /\ setPublished' = setPublished
    /\ postSyncFrames' =
        IF selectNeeded
            THEN Append(Append(postSyncFrames, "SELECT"), "DEL")
            ELSE Append(postSyncFrames, "DEL")
    /\ selectNeeded' = FALSE
    /\ UNCHANGED phase
    /\ stepsRemaining' = stepsRemaining - 1

ReplicateSetAfterLazyDel ==
    /\ stepsRemaining > 0
    /\ phase \in {"SUBSCRIBED", "STREAMING"}
    /\ lazyDelPublished
    /\ ~setPublished
    /\ setPublished' = TRUE
    /\ postSyncFrames' =
        IF selectNeeded
            THEN Append(Append(postSyncFrames, "SELECT"), "SET")
            ELSE Append(postSyncFrames, "SET")
    /\ selectNeeded' = FALSE
    /\ UNCHANGED <<phase, keyState, lazyDelPublished>>
    /\ stepsRemaining' = stepsRemaining - 1

DoneStutter ==
    /\ stepsRemaining = 0 \/ setPublished
    /\ UNCHANGED Vars

Next ==
    \/ SyncSubscribe
    \/ EnterStreamingLoop
    \/ ObserveLiveGet
    \/ GetExpiresAndReplicatesDel
    \/ ReplicateSetAfterLazyDel
    \/ DoneStutter

SetReplicationSequenceMatchesTest ==
    setPublished =>
        postSyncFrames = <<"SELECT", "DEL", "SET">>

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

================================================================================
