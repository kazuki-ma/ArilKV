--------------------------- MODULE BlockingWakeupFairness ---------------------------
EXTENDS Naturals, Sequences

CONSTANTS Keys, Clients, Null, MaxSteps

VARIABLES waitQ, blockedOn, arrivalOrder, serveOrder, servedEver, stepsRemaining

Vars == <<waitQ, blockedOn, arrivalOrder, serveOrder, servedEver, stepsRemaining>>

SeqToSet(seq) == {seq[i] : i \in 1..Len(seq)}

NoDup(seq) ==
    \A i, j \in 1..Len(seq):
        i # j => seq[i] # seq[j]

IsPrefix(prefix, full) ==
    /\ Len(prefix) <= Len(full)
    /\ \A i \in 1..Len(prefix): prefix[i] = full[i]

TypeOK ==
    /\ waitQ \in [Keys -> Seq(Clients)]
    /\ blockedOn \in [Clients -> Keys \cup {Null}]
    /\ arrivalOrder \in [Keys -> Seq(Clients)]
    /\ serveOrder \in [Keys -> Seq(Clients)]
    /\ servedEver \subseteq Clients
    /\ stepsRemaining \in 0..MaxSteps

Init ==
    /\ waitQ = [k \in Keys |-> <<>>]
    /\ blockedOn = [c \in Clients |-> Null]
    /\ arrivalOrder = [k \in Keys |-> <<>>]
    /\ serveOrder = [k \in Keys |-> <<>>]
    /\ servedEver = {}
    /\ stepsRemaining = MaxSteps

Block(c, k) ==
    /\ stepsRemaining > 0
    /\ blockedOn[c] = Null
    /\ c \notin servedEver
    /\ c \notin SeqToSet(waitQ[k])
    /\ waitQ' = [waitQ EXCEPT ![k] = Append(@, c)]
    /\ blockedOn' = [blockedOn EXCEPT ![c] = k]
    /\ arrivalOrder' = [arrivalOrder EXCEPT ![k] = Append(@, c)]
    /\ serveOrder' = serveOrder
    /\ servedEver' = servedEver
    /\ stepsRemaining' = stepsRemaining - 1

WakeHead(k) ==
    /\ stepsRemaining > 0
    /\ waitQ[k] # <<>>
    /\ LET c == Head(waitQ[k])
       IN /\ waitQ' = [waitQ EXCEPT ![k] = Tail(@)]
          /\ blockedOn' = [blockedOn EXCEPT ![c] = Null]
          /\ arrivalOrder' = arrivalOrder
          /\ serveOrder' = [serveOrder EXCEPT ![k] = Append(@, c)]
          /\ servedEver' = servedEver \cup {c}
          /\ stepsRemaining' = stepsRemaining - 1

PushNoWaiter(k) ==
    /\ stepsRemaining > 0
    /\ waitQ[k] = <<>>
    /\ UNCHANGED <<waitQ, blockedOn, arrivalOrder, serveOrder, servedEver>>
    /\ stepsRemaining' = stepsRemaining - 1

Next ==
    \/ \E c \in Clients, k \in Keys : Block(c, k)
    \/ \E k \in Keys : WakeHead(k)
    \/ \E k \in Keys : PushNoWaiter(k)
    \/ /\ stepsRemaining = 0
       /\ UNCHANGED Vars

InvQueueNoDup ==
    \A k \in Keys: NoDup(waitQ[k])

InvBlockedOnMatchesQueues ==
    \A c \in Clients:
      /\ blockedOn[c] = Null =>
           ~(\E k \in Keys: c \in SeqToSet(waitQ[k]))
      /\ blockedOn[c] # Null =>
           c \in SeqToSet(waitQ[blockedOn[c]])

InvServeOrderFIFO ==
    \A k \in Keys:
        IsPrefix(serveOrder[k], arrivalOrder[k])

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

================================================================================
