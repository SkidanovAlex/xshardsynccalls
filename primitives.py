
VERBOSE = True


def winning_tx(tx1, tx2):
    return tx1 if tx1.tx_id < tx2.tx_id else tx2


class Shard(object):
    def __init__(self, shard_id, parent_id, child_ids):
        super(Shard, self).__init__()
        self.shard_id = shard_id
        self.parent_id = parent_id
        self.child_ids = child_ids
        self.blocks = []

    def process_message(self, msg, outbox, pending_txs, locks):
        if msg.msg_type == Message.EXECUTE:
            self.process_tx(msg.tx, outbox, pending_txs, locks)
        elif msg.msg_type == Message.RETURN:
            self.process_return(msg.tx, outbox, pending_txs, locks)
        elif msg.msg_type == Message.BLOCKED:
            self.process_blocked(msg.tx, msg.tx_on, outbox)
        elif msg.msg_type == Message.ROLLBACK:
            # TODO
            pass
        else:
            assert False, "Unknown message type %s" % msg.msg_type

    def process_blocked(self, tx, tx_on, outbox):
        outgoing_msg = Message_Blocked(tx, tx_on)

        if outgoing_msg.target_shard_id == self.shard_id:
            # TODO
            pass

        else:
            if outgoing_msg.target_shard_id not in outbox:
                outbox[outgoing_msg.target_shard_id] = []
            outbox[outgoing_msg.target_shard_id].append(outgoing_msg)

    def process_tx(self, tx, outbox, pending_txs, locks):
        assert tx.steps[tx.step_id].shard_id == self.shard_id
        contract_id = tx.steps[tx.step_id].contract_id
        
        if contract_id in locks:
            pending_txs.append(tx)

            tx_on = locks[contract_id]

            if VERBOSE: print("%s(%s, %s) blocked on %s(%s, %s)" % (tx.tx_id, self.shard_id, tx.steps[tx.step_id].contract_id, tx_on.tx_id, tx_on.steps[tx_on.step_id].shard_id, tx_on.steps[tx_on.step_id].contract_id))

            self.process_blocked(tx, tx_on, outbox)
            return

        if tx.step_id + 1 == len(tx.steps):
            assert tx.step_id > 0 # txs with one step are useless for the demo
            outgoing_msg = Message_Return(Transaction.clone_tx(tx, tx.step_id - 1))
            if outgoing_msg.target_shard_id not in outbox:
                outbox[outgoing_msg.target_shard_id] = []
            outbox[outgoing_msg.target_shard_id].append(outgoing_msg)
        else:
            locks[contract_id] = tx

            outgoing_msg = Message_Execute(Transaction.clone_tx(tx, tx.step_id + 1))
            if outgoing_msg.target_shard_id not in outbox:
                outbox[outgoing_msg.target_shard_id] = []
            outbox[outgoing_msg.target_shard_id].append(outgoing_msg)

    def process_return(self, tx, outbox, pending_txs, locks):
        assert tx.steps[tx.step_id].shard_id == self.shard_id
        contract_id = tx.steps[tx.step_id].contract_id
        
        if tx.step_id == 0:
            pass # transaction is completed
        else:
            del locks[contract_id]

            outgoing_msg = Message_Return(Transaction.clone_tx(tx, tx.step_id - 1))
            if outgoing_msg.target_shard_id not in outbox:
                outbox[outgoing_msg.target_shard_id] = []
            outbox[outgoing_msg.target_shard_id].append(outgoing_msg)


class Block(object):
    def __init__(self, prev_block, outbox, locks, pending_txs):
        super(Block, self).__init__()
        self.prev_block = prev_block
        self.outbox = outbox
        self.locks = locks
        self.pending_txs = pending_txs


class Message(object):
    EXECUTE = 1
    RETURN = 2
    BLOCKED = 3
    ROLLBACK = 4

    def __init__(self, msg_type, target_shard_id):
        super(Message, self).__init__()
        self.msg_type = msg_type
        self.target_shard_id = target_shard_id


class Message_Execute(Message):
    def __init__(self, tx):
        super(Message_Execute, self).__init__(Message.EXECUTE, tx.steps[tx.step_id].shard_id)
        self.tx = tx


class Message_Return(Message):
    def __init__(self, tx):
        super(Message_Return, self).__init__(Message.RETURN, tx.steps[tx.step_id].shard_id)
        self.tx = tx


class Message_Blocked(Message):
    def __init__(self, tx, tx_on):
        super(Message_Blocked, self).__init__(Message.BLOCKED, winning_tx(tx, tx_on))
        self.tx = tx
        self.tx_on = tx_on


class Message_Rollback(Message):
    def __init__(self, tx):
        super(Message_Rollback, self).__init__(Message.ROLLBACK, tx.originating_shard_id)
        self.tx = tx


class Transaction(object):
    def __init__(self, tx_id, originating_shard_id, steps):
        super(Transaction, self).__init__()
        self.tx_id = tx_id
        self.originating_shard_id = originating_shard_id
        self.steps = steps
        self.step_id = 0

    @classmethod
    def clone_tx(cls, tx, step_id):
        ret = Transaction(tx.tx_id, tx.originating_shard_id, tx.steps)
        ret.step_id = step_id
        return ret


class Step(object):
    def __init__(self, shard_id, contract_id):
        super(Step, self).__init__()
        self.shard_id = shard_id
        self.contract_id = contract_id

