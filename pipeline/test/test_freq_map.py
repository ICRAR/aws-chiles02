"""

"""
import unittest

from casa_code.freq_map import freq_map

test_data_947 = [
    [
        "0",
        "EVLA_L#A0C0#0",
        "512",
        "TOPO",
        "947.623",
        "62.500",
        "32000.0",
        "963.5922",
        "12",
        "RR"
    ],
    [
        "1",
        "EVLA_L#A0C0#1",
        "512",
        "TOPO",
        "979.623",
        "62.500",
        "32000.0",
        "995.5922",
        "12",
        "RR"
    ],
    [
        "2",
        "EVLA_L#A0C0#2",
        "512",
        "TOPO",
        "1011.623",
        "62.500",
        "32000.0",
        "1027.5922",
        "12",
        "RR"
    ],
    [
        "3",
        "EVLA_L#A0C0#3",
        "512",
        "TOPO",
        "1043.623",
        "62.500",
        "32000.0",
        "1059.5922",
        "12",
        "RR"
    ],
    [
        "4",
        "EVLA_L#A0C0#4",
        "512",
        "TOPO",
        "1075.623",
        "62.500",
        "32000.0",
        "1091.5922",
        "12",
        "RR"
    ],
    [
        "5",
        "EVLA_L#A0C0#5",
        "512",
        "TOPO",
        "1107.623",
        "62.500",
        "32000.0",
        "1123.5922",
        "12",
        "RR"
    ],
    [
        "6",
        "EVLA_L#A0C0#6",
        "512",
        "TOPO",
        "1139.623",
        "62.500",
        "32000.0",
        "1155.5922",
        "12",
        "RR"
    ],
    [
        "7",
        "EVLA_L#A0C0#7",
        "512",
        "TOPO",
        "1171.623",
        "62.500",
        "32000.0",
        "1187.5922",
        "12",
        "RR"
    ],
    [
        "8",
        "EVLA_L#A0C0#8",
        "512",
        "TOPO",
        "1203.623",
        "62.500",
        "32000.0",
        "1219.5922",
        "12",
        "RR"
    ],
    [
        "9",
        "EVLA_L#A0C0#9",
        "512",
        "TOPO",
        "1235.623",
        "62.500",
        "32000.0",
        "1251.5922",
        "12",
        "RR"
    ],
    [
        "10",
        "EVLA_L#A0C0#10",
        "512",
        "TOPO",
        "1267.623",
        "62.500",
        "32000.0",
        "1283.5922",
        "12",
        "RR"
    ],
    [
        "11",
        "EVLA_L#A0C0#11",
        "512",
        "TOPO",
        "1299.623",
        "62.500",
        "32000.0",
        "1315.5922",
        "12",
        "RR"
    ],
    [
        "12",
        "EVLA_L#A0C0#12",
        "512",
        "TOPO",
        "1331.623",
        "62.500",
        "32000.0",
        "1347.5922",
        "12",
        "RR"
    ],
    [
        "13",
        "EVLA_L#A0C0#13",
        "512",
        "TOPO",
        "1363.623",
        "62.500",
        "32000.0",
        "1379.5922",
        "12",
        "RR"
    ],
    [
        "14",
        "EVLA_L#A0C0#14",
        "512",
        "TOPO",
        "1395.623",
        "62.500",
        "32000.0",
        "1411.5922",
        "12",
        "RR"
    ]
]


class TestFreqMap(unittest.TestCase):
    def test_001(self):
        spw, number_channels, width = freq_map(940, 960, test_data_947)
        self.assertEqual('0~0', spw)
        self.assertEqual(512, number_channels)
        self.assertEqual(62.5, width)

    def test_002(self):
        spw, number_channels, width = freq_map(940, 980, test_data_947)
        self.assertEqual('0~1', spw)
        self.assertEqual(512, number_channels)
        self.assertEqual(62.5, width)

    def test_003(self):
        spw, number_channels, width = freq_map(1360, 1420, test_data_947)
        self.assertEqual('12~14', spw)
        self.assertEqual(512, number_channels)
        self.assertEqual(62.5, width)
