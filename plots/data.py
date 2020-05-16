DATA = {
    'local': {
        'map_reduce': {
            'job1': (31, 41, 52, 66, 79, 91, 105, 130, 260, 525),
            'job2': (33, 46, 65, 92, 110, 126, 168, 190, 325, 690),
            'job3': (28, 37, 43, 56, 65, 74, 87, 98, 159, 255)
        },
        'spark': {
            # last one computation is too heavy for our computers
            'job1': (36, 51, 71, 93, 110, 130, 160, 176, 349, 0),
            'job2': (29, 49, 79, 90, 117, 132, 160, 177, 366, 0),
            'job3': (25, 39, 58, 69, 89, 100, 121, 138, 261, 0)
        },
        'hive': {
            'job1': (115, 130, 145, 175, 195, 215, 245, 299, 440, 810),
            'job2': (195, 220, 245, 285, 320, 355, 390, 507, 928, 1865),
            'job3': (240, 250, 275, 300, 332, 355, 375, 454, 676, 1074)
        }
    },
    'remote': {
        'map_reduce': {
            'job1': (30, 35, 38, 42, 48, 58, 66, 68, 126, 213),
            'job2': (34, 38, 44, 48, 54, 63, 67, 69, 137, 210),
            'job3': (31, 34, 36, 38, 41, 44, 50, 58, 98, 142)
        },
        'spark': {
            'job1': (43, 59, 77, 99, 138, 158, 163, 174, 308, 592),
            'job2': (47, 62, 81, 95, 129, 146, 166, 174, 311, 605),
            'job3': (44, 60, 72, 88, 125, 140, 143, 150, 267, 515)
        },
        'hive': {
            'job1': (120, 144, 152, 164, 176, 190, 232, 238, 303, 497),
            'job2': (210, 238, 249, 297, 297, 297, 325, 355, 581, 1112),
            'job3': (275, 275, 296, 309, 301, 315, 340, 346, 448, 625)
        }
    }
}
