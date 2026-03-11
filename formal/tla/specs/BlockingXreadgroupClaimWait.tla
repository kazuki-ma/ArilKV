----------------------- MODULE BlockingXreadgroupClaimWait -----------------------
EXTENDS Naturals, Sequences

CONSTANT UseClaimReady

VARIABLES queue, hasNewEntry, pendingExists, pendingIdleReached, waiter1State, waiter2State

Vars == <<queue, hasNewEntry, pendingExists, pendingIdleReached, waiter1State, waiter2State>>

HeadOrZero(q) == IF Len(q) = 0 THEN 0 ELSE Head(q)

HeadReady ==
    \/ /\ HeadOrZero(queue) \in {1, 2}
       /\ hasNewEntry
    \/ /\ HeadOrZero(queue) = 2
       /\ waiter2State = "BLOCKED"
       /\ pendingExists
       /\ pendingIdleReached
       /\ UseClaimReady

TypeOK ==
    /\ queue \in Seq({1, 2})
    /\ hasNewEntry \in BOOLEAN
    /\ pendingExists \in BOOLEAN
    /\ pendingIdleReached \in BOOLEAN
    /\ waiter1State \in {"BLOCKED", "DELIVERED"}
    /\ waiter2State \in {"BLOCKED", "DELIVERED"}

Init ==
    /\ queue = <<1, 2>>
    /\ hasNewEntry = FALSE
    /\ pendingExists = FALSE
    /\ pendingIdleReached = FALSE
    /\ waiter1State = "BLOCKED"
    /\ waiter2State = "BLOCKED"

WriteNewEntry ==
    /\ ~hasNewEntry
    /\ ~pendingExists
    /\ hasNewEntry' = TRUE
    /\ UNCHANGED <<queue, pendingExists, pendingIdleReached, waiter1State, waiter2State>>

WakeFirstOnNewEntry ==
    /\ queue = <<1, 2>>
    /\ waiter1State = "BLOCKED"
    /\ hasNewEntry
    /\ queue' = <<2>>
    /\ hasNewEntry' = FALSE
    /\ pendingExists' = TRUE
    /\ pendingIdleReached' = FALSE
    /\ waiter1State' = "DELIVERED"
    /\ UNCHANGED waiter2State

IdleThresholdReached ==
    /\ queue = <<2>>
    /\ pendingExists
    /\ ~pendingIdleReached
    /\ pendingIdleReached' = TRUE
    /\ UNCHANGED <<queue, hasNewEntry, pendingExists, waiter1State, waiter2State>>

\* TLA+ : ClaimWaitReady
ClaimWaitReady ==
    /\ queue = <<2>>
    /\ waiter2State = "BLOCKED"
    /\ pendingExists
    /\ pendingIdleReached
    /\ UseClaimReady

WakeSecondOnClaim ==
    /\ ClaimWaitReady
    /\ queue' = <<>>
    /\ waiter2State' = "DELIVERED"
    /\ UNCHANGED <<hasNewEntry, pendingExists, pendingIdleReached, waiter1State>>

Completed ==
    /\ queue = <<>>
    /\ UNCHANGED Vars

Next ==
    \/ WriteNewEntry
    \/ WakeFirstOnNewEntry
    \/ IdleThresholdReached
    \/ WakeSecondOnClaim
    \/ Completed

InvClaimablePendingHeadMustBeReady ==
    /\ queue = <<2>>
    /\ waiter2State = "BLOCKED"
    /\ pendingExists
    /\ pendingIdleReached
    => HeadReady

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

=================================================================================
