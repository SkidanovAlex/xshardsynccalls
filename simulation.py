from primitives import *
from scenarios import scenarios
import sys, json


MAX_STEPS = 20


class TxState(object):
    def __init__(self, tx):
        super(TxState, self).__init__()
        self.tx = tx
        self.reset()

    def reset(self):
        self.latest_step = -1
        self.latest_rollback_forward = -1
        self.latest_rollback_backward = len(self.tx.steps)
        self.latest_return = len(self.tx.steps)
        

class Simulation(object):
    def __init__(self, configuration, transactions):
        super(Simulation, self).__init__()
        self.shards = []
        self.txstates = []

        self.log = []
        
        parents_map = {}
        for shard_id, children in configuration.items():
            for child_id in children:
                parents_map[child_id] = shard_id

        for shard_id, children in configuration.items():
            parent_id = parents_map[shard_id] if shard_id in parents_map else None
            shard = Shard(shard_id, parent_id, children)
            shard.callback = lambda x: self.on_message(x)
            self.shards.append(shard)
            shard.blocks.append(Block(None, [], {}, {}, [], {}))

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
            self.txstates[-1].latest_step = 0

            shard = self.shards[tx.originating_shard_id]
            shard.process_tx(tx, shard.blocks[-1].outbox, shard.blocks[-1].pending_txs, shard.blocks[-1].locks, shard.blocks[-1].graph)

        self.create_log_entry()

    def create_log_entry(self):
        log_entry = {'shards': [], 'txs': []}
        for shard in self.shards:
            shard_log_entry = {}
            shard_log_entry['graph'] = list(shard.blocks[-1].graph.keys())
            shard_log_entry['blocks'] = []
            for block in shard.blocks:
                block_log_entry = ""
                for msg in block.inbox:
                    block_log_entry += '<b>%s</b>\n' % str(msg)
                for target_shard_id in block.outbox:
                    for msg in block.outbox[target_shard_id]:
                        block_log_entry += '%s -> %s\n' % (str(msg), target_shard_id)
                shard_log_entry['blocks'].append(block_log_entry)
            log_entry['shards'].append(shard_log_entry)
        for txstate in self.txstates:
            tx_log_entry = {}
            tx_log_entry['steps'] = [[x.shard_id, x.contract_id] for x in txstate.tx.steps]
            tx_log_entry['stats'] = [txstate.latest_step, txstate.latest_return, txstate.latest_rollback_forward, txstate.latest_rollback_backward]
            log_entry['txs'].append(tx_log_entry)
        self.log.append(log_entry)

    def is_finished(self):
        for txstate in self.txstates:
            if txstate.latest_step + 1 < len(txstate.tx.steps) or txstate.latest_return > 0:
                return False

        return True


    def on_message(self, msg):
        if msg.msg_type == Message.EXECUTE:
            self.txstates[msg.tx.tx_id].latest_step = max(self.txstates[msg.tx.tx_id].latest_step, msg.tx.step_id)
        if msg.msg_type == Message.ROLLBACK_FORWARD:
            self.txstates[msg.tx.tx_id].latest_rollback_forward = max(self.txstates[msg.tx.tx_id].latest_rollback_forward, msg.tx.step_id)
            # some hackery on the next line...
            if self.txstates[msg.tx.tx_id].latest_rollback_forward > self.txstates[msg.tx.tx_id].latest_step:
                self.txstates[msg.tx.tx_id].latest_rollback_forward = -1
        if msg.msg_type == Message.ROLLBACK_BACKWARD:
            self.txstates[msg.tx.tx_id].latest_rollback_backward = min(self.txstates[msg.tx.tx_id].latest_rollback_backward, msg.tx.step_id)
        if msg.msg_type == Message.RETURN:
            self.txstates[msg.tx.tx_id].latest_return = min(self.txstates[msg.tx.tx_id].latest_return, msg.tx.step_id)


    def step(self):
        if VERBOSE: print("EXECUTING A STEP")
        new_blocks = {}
        for shard in self.shards:
            inbox = []
            outbox = {}
            pending_txs = []
            locks = {k: v for (k, v) in shard.blocks[-1].locks.items()}
            graph = {}

            for tx in shard.blocks[-1].pending_txs:
                shard.process_tx(tx, outbox, pending_txs, locks, graph)
                self.on_message(Message_Execute(tx))

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
                    inbox.append(msg)
                    shard.process_message(msg, outbox, pending_txs, locks, graph)

            new_blocks[shard.shard_id] = Block(shard.blocks[-1], inbox, outbox, locks, pending_txs, graph)

        for shard in self.shards:
            shard.blocks.append(new_blocks[shard.shard_id])

        self.create_log_entry()

        for txstate in self.txstates:
            if txstate.latest_rollback_backward == 0:
                txstate.reset()


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

    with open('log.js', 'w') as f:
        f.write('v = %s' % json.dumps(simulation.log))

