
class Shard(object):
    def __init__(self, shard_id, parent_id, child_ids):
        super(Shard, self).__init__()
        self.shard_id = shard_id
        self.parent_id = parent_id
        self.child_ids = child_ids


class Block(object):
    def __init__(self, prev_block, sources, inbox, outbox):
        super(Block, self).__init__()
        self.prev_block = prev_block
        self.sources = sources
        self.inbox = inbox
        self.outbox = outbox


class Message(object):
    EXECUTE = 1
    BLOCKED = 2
    ROLLBACK = 3

    def __init__(self, msg_type, target_shard_id):
        super(Message, self).__init__()
        self.msg_type = msg_type
        self.target_shard_id = target_shard_id


class Message_Execute(Message):
    def __init__(self, tx):
        super(Message_Execute, self).__init__(Message.EXECUTE, tx.steps[tx.step_id].shard_id)
        self.tx = tx
        self.step = step


class Message_Return(Message):
    def __init__(self, tx):
        super(Message_Return, self).__init__(Message.EXECUTE, tx.steps[tx.step_id].shard_id)
        self.tx = tx


class Message_Blocked(Message):
    def __init__(self, tx, tx_on):
        super(Message_Blocked, self).__init__(Message.BLOCKED, )
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
        self.step = 0


class Step(object):
    def __init__(self, shard_id, contract_id):
        super(Step, self).__init__()
        self.shard_id = shard_id
        self.contract_id = contract_id

