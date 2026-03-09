------------------------ MODULE BlockingWaitClassIsolation ------------------------
EXTENDS Naturals, Sequences

CONSTANTS SharedKeyQueue, MaxSteps

Keys == {"k"}
Classes == {"list", "stream"}
Clients == {"listClient", "streamClient"}
Null == "null"

VARIABLES waitQ, blockedKey, blockedClass, ready, delivered, stepsRemaining

Vars == <<waitQ, blockedKey, blockedClass, ready, delivered, stepsRemaining>>

QueueId(k, cl) == IF SharedKeyQueue THEN <<k, "shared">> ELSE <<k, cl>>

QueueIds ==
    IF SharedKeyQueue
        THEN {<<k, "shared">> : k \in Keys}
        ELSE {<<k, cl>> : k \in Keys, cl \in Classes}

SeqToSet(seq) == {seq[i] : i \in 1..Len(seq)}

TypeOK ==
    /\ waitQ \in [QueueIds -> Seq(Clients)]
    /\ blockedKey \in [Clients -> Keys \cup {Null}]
    /\ blockedClass \in [Clients -> Classes \cup {Null}]
    /\ ready \in [Keys -> [Classes -> BOOLEAN]]
    /\ delivered \subseteq Clients
    /\ stepsRemaining \in 0..MaxSteps

Init ==
    /\ waitQ = [qid \in QueueIds |-> <<>>]
    /\ blockedKey = [c \in Clients |-> Null]
    /\ blockedClass = [c \in Clients |-> Null]
    /\ ready = [k \in Keys |-> [cl \in Classes |-> FALSE]]
    /\ delivered = {}
    /\ stepsRemaining = MaxSteps

Block(c, k, cl) ==
    /\ stepsRemaining > 0
    /\ blockedKey[c] = Null
    /\ blockedClass[c] = Null
    /\ c \notin delivered
    /\ LET qid == QueueId(k, cl)
       IN /\ c \notin SeqToSet(waitQ[qid])
          /\ waitQ' = [waitQ EXCEPT ![qid] = Append(@, c)]
    /\ blockedKey' = [blockedKey EXCEPT ![c] = k]
    /\ blockedClass' = [blockedClass EXCEPT ![c] = cl]
    /\ UNCHANGED <<ready, delivered>>
    /\ stepsRemaining' = stepsRemaining - 1

SignalReady(k, cl) ==
    /\ stepsRemaining > 0
    /\ ready[k][cl] = FALSE
    /\ ready' = [ready EXCEPT ![k][cl] = TRUE]
    /\ UNCHANGED <<waitQ, blockedKey, blockedClass, delivered>>
    /\ stepsRemaining' = stepsRemaining - 1

ServeReadyHead(c, k, cl) ==
    /\ stepsRemaining > 0
    /\ blockedKey[c] = k
    /\ blockedClass[c] = cl
    /\ ready[k][cl]
    /\ LET qid == QueueId(k, cl)
       IN /\ waitQ[qid] # <<>>
          /\ Head(waitQ[qid]) = c
          /\ waitQ' = [waitQ EXCEPT ![qid] = Tail(@)]
    /\ blockedKey' = [blockedKey EXCEPT ![c] = Null]
    /\ blockedClass' = [blockedClass EXCEPT ![c] = Null]
    /\ delivered' = delivered \cup {c}
    /\ UNCHANGED ready
    /\ stepsRemaining' = stepsRemaining - 1

Next ==
    \/ Block("listClient", "k", "list")
    \/ Block("streamClient", "k", "stream")
    \/ SignalReady("k", "stream")
    \/ ServeReadyHead("listClient", "k", "list")
    \/ ServeReadyHead("streamClient", "k", "stream")
    \/ /\ stepsRemaining = 0
       /\ UNCHANGED Vars

StreamReadyBlockedBehindForeignHead ==
    /\ ready["k"]["stream"]
    /\ blockedKey["streamClient"] = "k"
    /\ blockedClass["streamClient"] = "stream"
    /\ LET qid == QueueId("k", "stream")
       IN /\ waitQ[qid] # <<>>
          /\ Head(waitQ[qid]) # "streamClient"

NoCrossClassStarvation ==
    ~StreamReadyBlockedBehindForeignHead

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

=============================================================================
