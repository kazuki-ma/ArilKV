---------------------------- MODULE BlockingDisconnectLeak ----------------------------
EXTENDS Naturals, Sequences

CONSTANTS Keys, Clients, Null, MaxSteps, DisconnectCleansUp

States == {"ACTIVE", "BLOCKED", "DISCONNECTED"}

VARIABLES waitQ, status, blockedOn, stepsRemaining

Vars == <<waitQ, status, blockedOn, stepsRemaining>>

SeqToSet(seq) == {seq[i] : i \in 1..Len(seq)}

NoDup(seq) ==
    \A i, j \in 1..Len(seq):
        i # j => seq[i] # seq[j]

TypeOK ==
    /\ waitQ \in [Keys -> Seq(Clients)]
    /\ status \in [Clients -> States]
    /\ blockedOn \in [Clients -> Keys \cup {Null}]
    /\ stepsRemaining \in 0..MaxSteps
    /\ DisconnectCleansUp \in BOOLEAN

Init ==
    /\ waitQ = [k \in Keys |-> <<>>]
    /\ status = [c \in Clients |-> "ACTIVE"]
    /\ blockedOn = [c \in Clients |-> Null]
    /\ stepsRemaining = MaxSteps

Block(c, k) ==
    /\ stepsRemaining > 0
    /\ status[c] = "ACTIVE"
    /\ c \notin SeqToSet(waitQ[k])
    /\ waitQ' = [waitQ EXCEPT ![k] = Append(@, c)]
    /\ status' = [status EXCEPT ![c] = "BLOCKED"]
    /\ blockedOn' = [blockedOn EXCEPT ![c] = k]
    /\ stepsRemaining' = stepsRemaining - 1

Disconnect(c) ==
    /\ stepsRemaining > 0
    /\ status[c] \in {"ACTIVE", "BLOCKED"}
    /\ status' = [status EXCEPT ![c] = "DISCONNECTED"]
    /\ blockedOn' = [blockedOn EXCEPT ![c] = Null]
    /\ waitQ' =
        IF status[c] = "BLOCKED" /\ DisconnectCleansUp
          THEN [waitQ EXCEPT ![blockedOn[c]] = SelectSeq(@, LAMBDA x: x # c)]
          ELSE waitQ
    /\ stepsRemaining' = stepsRemaining - 1

PushWake(k) ==
    /\ stepsRemaining > 0
    /\ IF waitQ[k] # <<>> /\ status[Head(waitQ[k])] = "BLOCKED"
         THEN LET c == Head(waitQ[k])
              IN /\ waitQ' = [waitQ EXCEPT ![k] = Tail(@)]
                 /\ status' = [status EXCEPT ![c] = "ACTIVE"]
                 /\ blockedOn' = [blockedOn EXCEPT ![c] = Null]
         ELSE /\ UNCHANGED <<waitQ, status, blockedOn>>
    /\ stepsRemaining' = stepsRemaining - 1

Next ==
    \/ \E c \in Clients, k \in Keys : Block(c, k)
    \/ \E c \in Clients : Disconnect(c)
    \/ \E k \in Keys : PushWake(k)
    \/ /\ stepsRemaining = 0
       /\ UNCHANGED Vars

InvQueueNoDup ==
    \A k \in Keys: NoDup(waitQ[k])

InvBlockedStatusAgreement ==
    \A c \in Clients:
      /\ status[c] = "BLOCKED" <=> blockedOn[c] # Null
      /\ status[c] # "BLOCKED" => blockedOn[c] = Null

InvQueueContainsOnlyBlockedClients ==
    \A k \in Keys:
      \A i \in 1..Len(waitQ[k]):
        /\ status[waitQ[k][i]] = "BLOCKED"
        /\ blockedOn[waitQ[k][i]] = k

InvBlockedClientInOnlyItsQueue ==
    \A c \in Clients:
      status[c] = "BLOCKED" =>
        \A k \in Keys:
          k # blockedOn[c] => c \notin SeqToSet(waitQ[k])

InvNoDisconnectedHeadWithLiveFollower ==
    \A k \in Keys:
      ~( /\ Len(waitQ[k]) >= 2
         /\ status[Head(waitQ[k])] = "DISCONNECTED"
         /\ \E i \in 2..Len(waitQ[k]): status[waitQ[k][i]] = "BLOCKED")

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

================================================================================
