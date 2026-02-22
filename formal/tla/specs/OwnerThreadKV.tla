------------------------------ MODULE OwnerThreadKV ------------------------------
EXTENDS Naturals, Sequences

CONSTANTS Keys, Owners, Values, Null, MaxSteps

RequestType == [kind : {"set", "get"}, key : Keys, value : Values \cup {Null}]

VARIABLES ownerOf, store, reqQ, blocked, stepsRemaining

Vars == <<ownerOf, store, reqQ, blocked, stepsRemaining>>

TypeOK ==
    /\ ownerOf \in [Keys -> Owners]
    /\ store \in [Keys -> Values \cup {Null}]
    /\ reqQ \in [Owners -> Seq(RequestType)]
    /\ blocked \subseteq Keys
    /\ stepsRemaining \in 0..MaxSteps

Init ==
    /\ ownerOf \in [Keys -> Owners]
    /\ store = [k \in Keys |-> Null]
    /\ reqQ = [o \in Owners |-> <<>>]
    /\ blocked = {}
    /\ stepsRemaining = MaxSteps

EnqueueSet(k, v) ==
    LET o == ownerOf[k]
        req == [kind |-> "set", key |-> k, value |-> v]
    IN  /\ stepsRemaining > 0
        /\ reqQ' = [reqQ EXCEPT ![o] = Append(@, req)]
        /\ stepsRemaining' = stepsRemaining - 1
        /\ UNCHANGED <<ownerOf, store, blocked>>

EnqueueGet(k) ==
    LET o == ownerOf[k]
        req == [kind |-> "get", key |-> k, value |-> Null]
    IN  /\ stepsRemaining > 0
        /\ reqQ' = [reqQ EXCEPT ![o] = Append(@, req)]
        /\ stepsRemaining' = stepsRemaining - 1
        /\ UNCHANGED <<ownerOf, store, blocked>>

OwnerStep(o) ==
    /\ reqQ[o] # <<>>
    /\ stepsRemaining > 0
    /\ LET req == Head(reqQ[o])
       IN /\ reqQ' = [reqQ EXCEPT ![o] = Tail(@)]
          /\ stepsRemaining' = stepsRemaining - 1
          /\ IF req.kind = "set"
                THEN /\ store' = [store EXCEPT ![req.key] = req.value]
                     /\ blocked' = blocked \ {req.key}
                ELSE /\ store' = store
                     /\ blocked' = blocked
          /\ ownerOf' = ownerOf

Next ==
    \/ \E k \in Keys, v \in Values : EnqueueSet(k, v)
    \/ \E k \in Keys : EnqueueGet(k)
    \/ \E o \in Owners : OwnerStep(o)
    \/ /\ stepsRemaining = 0
       /\ UNCHANGED Vars

InvQueueOwner ==
    \A o \in Owners:
        \A i \in 1..Len(reqQ[o]):
            ownerOf[reqQ[o][i].key] = o

InvStoreDomain ==
    DOMAIN store = Keys

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

=============================================================================
