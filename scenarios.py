
scenarios = {
    '2nodeadlocks': {
        'shards_config': {
            0: [1], 1: []
        },
        'transactions': [
            [(0, 'A'), (1, 'B')],
        ]
    },
    '2easy': {
        'shards_config': {
            0: [1], 1: []
        },
        'transactions': [
            [(0, 'A'), (1, 'B')],
            [(1, 'B'), (0, 'A')]
        ]
    }
}

