-------------------------- MODULE LinkedBlmoveChainResidue --------------------------
EXTENDS Naturals

\* Model target:
\* `linked_blmove_chain_is_observable_without_intermediate_residue`
\*
\* Test intent:
\* 1) waiter-1 blocks on BLMOVE list1 -> list2
\* 2) waiter-2 blocks on BLMOVE list2 -> list3
\* 3) producer RPUSH list1 foo
\* 4) immediately after producer gets reply, inspector should observe:
\*      list1 = empty, list2 = empty, list3 = [foo]
\*
\* Risks modeled here:
\* - Producer ACK can be exposed before wakeup chain drain.
\* - Scheduler can defer inspector until after unrelated actions, masking residue.
\*
\* Guard toggles:
\* - WaitForBlockingDrainBeforeAck:
\*     conservative guard (both waiters done before ACK).
\* - WaitForTailDrainBeforeAck:
\*     minimal guard for this chain (waiter-2 done before ACK).
\* - ObserveImmediatelyAfterAck:
\*     force inspector action as the immediate step after ACK.

CONSTANTS
    MaxSteps,
    WaitForBlockingDrainBeforeAck,
    WaitForTailDrainBeforeAck,
    ObserveImmediatelyAfterAck

WaiterStates == {"NEW", "BLOCKED", "DONE"}

VARIABLES
    waiter1,
    waiter2,
    list1HasFoo,
    list2HasFoo,
    list3HasFoo,
    pushed,
    producerAcked,
    observed,
    observedList1HasFoo,
    observedList2HasFoo,
    observedList3HasFoo,
    stepsRemaining

Vars ==
    <<waiter1, waiter2, list1HasFoo, list2HasFoo, list3HasFoo, pushed, producerAcked, observed,
      observedList1HasFoo, observedList2HasFoo, observedList3HasFoo, stepsRemaining>>

BoolToNat(v) == IF v THEN 1 ELSE 0

TokenCount ==
    BoolToNat(list1HasFoo) + BoolToNat(list2HasFoo) + BoolToNat(list3HasFoo)

TypeOK ==
    /\ waiter1 \in WaiterStates
    /\ waiter2 \in WaiterStates
    /\ list1HasFoo \in BOOLEAN
    /\ list2HasFoo \in BOOLEAN
    /\ list3HasFoo \in BOOLEAN
    /\ pushed \in BOOLEAN
    /\ producerAcked \in BOOLEAN
    /\ observed \in BOOLEAN
    /\ observedList1HasFoo \in BOOLEAN
    /\ observedList2HasFoo \in BOOLEAN
    /\ observedList3HasFoo \in BOOLEAN
    /\ stepsRemaining \in 0..MaxSteps
    /\ WaitForBlockingDrainBeforeAck \in BOOLEAN
    /\ WaitForTailDrainBeforeAck \in BOOLEAN
    /\ ObserveImmediatelyAfterAck \in BOOLEAN
    /\ ~(WaitForBlockingDrainBeforeAck /\ WaitForTailDrainBeforeAck)

TokenConservation ==
    IF pushed
        THEN TokenCount = 1
        ELSE TokenCount = 0

ObservedNoIntermediateResidue ==
    observed =>
        /\ ~observedList1HasFoo
        /\ ~observedList2HasFoo
        /\ observedList3HasFoo

InspectorPending ==
    producerAcked /\ ~observed

OtherActionsAllowed ==
    ~(ObserveImmediatelyAfterAck /\ InspectorPending)

AckGuardSatisfied ==
    IF WaitForBlockingDrainBeforeAck
        THEN /\ waiter1 = "DONE"
             /\ waiter2 = "DONE"
    ELSE IF WaitForTailDrainBeforeAck
        THEN waiter2 = "DONE"
    ELSE TRUE

Init ==
    /\ waiter1 = "NEW"
    /\ waiter2 = "NEW"
    /\ list1HasFoo = FALSE
    /\ list2HasFoo = FALSE
    /\ list3HasFoo = FALSE
    /\ pushed = FALSE
    /\ producerAcked = FALSE
    /\ observed = FALSE
    /\ observedList1HasFoo = FALSE
    /\ observedList2HasFoo = FALSE
    /\ observedList3HasFoo = FALSE
    /\ stepsRemaining = MaxSteps

BlockWaiter1 ==
    /\ stepsRemaining > 0
    /\ OtherActionsAllowed
    /\ waiter1 = "NEW"
    /\ waiter1' = "BLOCKED"
    /\ UNCHANGED
        <<waiter2, list1HasFoo, list2HasFoo, list3HasFoo, pushed, producerAcked, observed,
          observedList1HasFoo, observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

BlockWaiter2 ==
    /\ stepsRemaining > 0
    /\ OtherActionsAllowed
    /\ waiter2 = "NEW"
    /\ waiter2' = "BLOCKED"
    /\ UNCHANGED
        <<waiter1, list1HasFoo, list2HasFoo, list3HasFoo, pushed, producerAcked, observed,
          observedList1HasFoo, observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

ProducerRpushList1 ==
    /\ stepsRemaining > 0
    /\ OtherActionsAllowed
    /\ ~pushed
    /\ waiter1 = "BLOCKED"
    /\ waiter2 = "BLOCKED"
    /\ pushed' = TRUE
    /\ list1HasFoo' = TRUE
    /\ UNCHANGED
        <<waiter1, waiter2, list2HasFoo, list3HasFoo, producerAcked, observed,
          observedList1HasFoo, observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

WakeWaiter1Blmove ==
    /\ stepsRemaining > 0
    /\ OtherActionsAllowed
    /\ waiter1 = "BLOCKED"
    /\ list1HasFoo
    /\ waiter1' = "DONE"
    /\ list1HasFoo' = FALSE
    /\ list2HasFoo' = TRUE
    /\ UNCHANGED
        <<waiter2, list3HasFoo, pushed, producerAcked, observed, observedList1HasFoo,
          observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

WakeWaiter2Blmove ==
    /\ stepsRemaining > 0
    /\ OtherActionsAllowed
    /\ waiter2 = "BLOCKED"
    /\ list2HasFoo
    /\ waiter2' = "DONE"
    /\ list2HasFoo' = FALSE
    /\ list3HasFoo' = TRUE
    /\ UNCHANGED
        <<waiter1, list1HasFoo, pushed, producerAcked, observed, observedList1HasFoo,
          observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

ProducerAck ==
    /\ stepsRemaining > 0
    /\ OtherActionsAllowed
    /\ pushed
    /\ ~producerAcked
    /\ AckGuardSatisfied
    /\ producerAcked' = TRUE
    /\ UNCHANGED
        <<waiter1, waiter2, list1HasFoo, list2HasFoo, list3HasFoo, pushed, observed,
          observedList1HasFoo, observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

ObservePostProducerAck ==
    /\ producerAcked
    /\ ~observed
    /\ observed' = TRUE
    /\ observedList1HasFoo' = list1HasFoo
    /\ observedList2HasFoo' = list2HasFoo
    /\ observedList3HasFoo' = list3HasFoo
    /\ UNCHANGED
        <<waiter1, waiter2, list1HasFoo, list2HasFoo, list3HasFoo, pushed, producerAcked>>
    /\ stepsRemaining' = IF stepsRemaining > 0 THEN stepsRemaining - 1 ELSE 0

DoneStutter ==
    /\ (stepsRemaining = 0 \/ observed)
    /\ OtherActionsAllowed
    /\ UNCHANGED Vars

Next ==
    \/ BlockWaiter1
    \/ BlockWaiter2
    \/ ProducerRpushList1
    \/ WakeWaiter1Blmove
    \/ WakeWaiter2Blmove
    \/ ProducerAck
    \/ ObservePostProducerAck
    \/ DoneStutter

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

================================================================================
