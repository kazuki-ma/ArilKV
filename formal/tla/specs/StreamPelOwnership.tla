---- MODULE StreamPelOwnership ----
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Consumers, EntryIds, NoConsumer, NoEntry

ASSUME NoConsumer \notin Consumers
ASSUME NoEntry \notin EntryIds

VARIABLES groupOwner, consumerPending, lastDelivered

Vars == << groupOwner, consumerPending, lastDelivered >>

Init ==
    /\ groupOwner = [e \in EntryIds |-> NoConsumer]
    /\ consumerPending = [c \in Consumers |-> {}]
    /\ lastDelivered = NoEntry

EnsureConsumerState(c) ==
    /\ c \in Consumers
    /\ UNCHANGED Vars

DeliverPendingOwnership(c, e) ==
    /\ c \in Consumers
    /\ e \in EntryIds
    /\ LET prev == groupOwner[e] IN
       /\ groupOwner' = [groupOwner EXCEPT ![e] = c]
       /\ consumerPending' =
            [cc \in Consumers |->
                IF cc = c THEN consumerPending[cc] \cup {e}
                ELSE IF prev \in Consumers /\ cc = prev
                    THEN consumerPending[cc] \ {e}
                    ELSE consumerPending[cc]
            ]
       /\ UNCHANGED lastDelivered

AckPendingEntry(c, e) ==
    /\ c \in Consumers
    /\ e \in EntryIds
    /\ groupOwner[e] = c
    /\ groupOwner' = [groupOwner EXCEPT ![e] = NoConsumer]
    /\ consumerPending' = [consumerPending EXCEPT ![c] = consumerPending[c] \ {e}]
    /\ UNCHANGED lastDelivered

AdvanceLastDeliveredId(e) ==
    /\ e \in EntryIds
    /\ lastDelivered' = e
    /\ UNCHANGED << groupOwner, consumerPending >>

Next ==
    \/ \E c \in Consumers : EnsureConsumerState(c)
    \/ \E c \in Consumers, e \in EntryIds : DeliverPendingOwnership(c, e)
    \/ \E c \in Consumers, e \in EntryIds : AckPendingEntry(c, e)
    \/ \E e \in EntryIds : AdvanceLastDeliveredId(e)

PendingOwnerMatchesConsumerSets ==
    \A e \in EntryIds :
        IF groupOwner[e] = NoConsumer THEN
            \A c \in Consumers : e \notin consumerPending[c]
        ELSE
            /\ e \in consumerPending[groupOwner[e]]
            /\ \A c \in Consumers :
                c # groupOwner[e] => e \notin consumerPending[c]

ConsumerSetsOnlyReferenceOwnedEntries ==
    \A c \in Consumers :
        \A e \in consumerPending[c] :
            groupOwner[e] = c

TypeInvariant ==
    /\ groupOwner \in [EntryIds -> Consumers \cup {NoConsumer}]
    /\ consumerPending \in [Consumers -> SUBSET EntryIds]
    /\ lastDelivered \in EntryIds \cup {NoEntry}

Spec == Init /\ [][Next]_Vars

THEOREM Spec => []TypeInvariant

====
