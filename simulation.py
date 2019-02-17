from primitives import *
from scenarios import scenarios
import sys


MAX_STEPS = 20


class TxState(object):
    def __init__(self, tx):
        super(TxState, self).__init__()
        self.tx = tx
        self.reset()

    def reset(self):
        self.latest_step = 0
        self.latest_rollback_forward = 0
        self.latest_rollback_backward = len(self.tx.steps)
        self.latest_return = len(self.tx.steps)
        

class Simulation(object):
    def __init__(self, configuration, transactions):
        super(Simulation, self).__init__()
        self.shards = []
        self.txstates = []
        
        parents_map = {}
        for shard_id, children in configuration.items():
            for child_id in children:
                parents_map[child_id] = shard_id

        for shard_id, children in configuration.items():
            parent_id = parents_map[shard_id] if shard_id in parents_map else None
            shard = Shard(shard_id, parent_id, children)
            self.shards.append(shard)
            shard.blocks.append(Block(None, {}, {}, [], {}))

        self.routers = [set() for _ in self.shards]
        children_closure = [set() for _ in configuration.items()]

        for shard_id in reversed(range(1, len(configuration.items()))):
            children_closure[parents_map[shard_id]].add(shard_id)
            for child in children_closure[shard_id]:
                children_closure[parents_map[shard_id]].add(child)
            children_closure[shard_id].add(shard_id)
        children_closure[0].add(0)

        for shard in self.shards:
            shard_id = shard.shard_id
            for child_id in shard.child_ids:
                for s1 in range(len(self.shards)):
                    for s2 in range(len(self.shards)):
                        if s1 != shard_id and s2 != shard_id and s1 in children_closure[child_id] and s2 not in children_closure[child_id]:
                            self.routers[shard_id].add((s1, s2))
                            self.routers[shard_id].add((s2, s1))

        print(self.routers[0])

        for i, steps in enumerate(transactions):
            tx = Transaction(i, steps[0][0], [Step(x[0], x[1]) for x in steps])
            self.txstates.append(TxState(tx))

            shard = self.shards[tx.originating_shard_id]
            shard.process_tx(tx, shard.blocks[-1].outbox, shard.blocks[-1].pending_txs, shard.blocks[-1].locks, shard.blocks[-1].graph)


    def is_finished(self):
        for txstate in self.txstates:
            if txstate.latest_step + 1 < len(txstate.tx.steps) or txstate.latest_return > 0:
                return False

        return True


    def step(self):
        if VERBOSE: print("EXECUTING A STEP")
        new_blocks = {}
        for shard in self.shards:
            outbox = {}
            pending_txs = []
            locks = {k: v for (k, v) in shard.blocks[-1].locks.items()}
            graph = {}

            for tx in shard.blocks[-1].pending_txs:
                shard.process_tx(tx, outbox, pending_txs, locks, graph)

            for neighbor_id in [shard.parent_id, shard.shard_id] + shard.child_ids:
                if neighbor_id is None:
                    continue

                # Routing
                for target_shard_id in self.shards[neighbor_id].blocks[-1].outbox:
                    if target_shard_id != shard.shard_id:
                        if (neighbor_id, target_shard_id) in self.routers[shard.shard_id]:
                            for msg in self.shards[neighbor_id].blocks[-1].outbox[target_shard_id]:
                                if target_shard_id not in outbox:
                                    outbox[target_shard_id] = []
                                outbox[target_shard_id].append(msg)

                if shard.shard_id not in self.shards[neighbor_id].blocks[-1].outbox:
                    continue

                # Message processing
                for msg in self.shards[neighbor_id].blocks[-1].outbox[shard.shard_id]:
                    shard.process_message(msg, outbox, pending_txs, locks, graph)
                    if msg.msg_type == Message.EXECUTE:
                        self.txstates[msg.tx.tx_id].latest_step = max(self.txstates[msg.tx.tx_id].latest_step, msg.tx.step_id)
                    if msg.msg_type == Message.ROLLBACK_FORWARD:
                        self.txstates[msg.tx.tx_id].latest_rollback_forward = max(self.txstates[msg.tx.tx_id].latest_rollback_forward, msg.tx.step_id)
                    if msg.msg_type == Message.ROLLBACK_BACKWARD:
                        self.txstates[msg.tx.tx_id].latest_rollback_backward = min(self.txstates[msg.tx.tx_id].latest_rollback_backward, msg.tx.step_id)
                        if self.txstates[msg.tx.tx_id].latest_rollback_backward == 0:
                            self.txstates[msg.tx.tx_id].reset()
                    if msg.msg_type == Message.RETURN:
                        self.txstates[msg.tx.tx_id].latest_return = min(self.txstates[msg.tx.tx_id].latest_return, msg.tx.step_id)

            new_blocks[shard.shard_id] = Block(shard.blocks[-1], outbox, locks, pending_txs, graph)

        for shard in self.shards:
            shard.blocks.append(new_blocks[shard.shard_id])


if __name__ == "__main__":
    scenario = scenarios[sys.argv[1]]
    simulation = Simulation(scenario['shards_config'], scenario['transactions'])

    for i in range(MAX_STEPS):
        if simulation.is_finished():
            break
        simulation.step()

    else:
        for txstate in simulation.txstates:
            print("TX %s: step %s, rollbacks %s %s, return %s" % (txstate.tx.tx_id, txstate.latest_step, txstate.latest_rollback_forward, txstate.latest_rollback_backward, txstate.latest_return))
        assert False, "Haven't finished in %s steps" % MAX_STEPS

