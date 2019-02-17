
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
    },
    '2interesting': {
        'shards_config': {
            0: [1], 1: []
        },
        'transactions': [
            [(0, 'A'), (1, 'B\''), (1, 'B')],
            [(1, 'B'), (0, 'A\''), (0, 'A')]
        ]
    },
    '3loop': {
        'shards_config': {
            0: [1, 2, 3], 1: [], 2: [], 3: []
        },
        'transactions': [
            [(1, 'A'), (2, 'B')],
            [(2, 'B'), (3, 'C')],
            [(3, 'C'), (1, 'A')],
        ]
    },
}

