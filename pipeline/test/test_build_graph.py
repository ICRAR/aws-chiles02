#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
"""
Test the building of graphs
"""
import unittest

from aws_chiles02.common import MeasurementSetData, FrequencyPair


class TestBuildGraph(unittest.TestCase):
    def setUp(self):
        self.work_to_do = {
            MeasurementSetData('13B-266.sb25387671.eb28662253.56678.261355925926', 107599503360):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27261805.eb28541939.56615.34480074074', 369835100160):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27261033.eb28593303.56652.30548047453', 175966218240):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27264735.eb28591716.56648.26500478009', 335832197120):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27258103.eb28612295.56662.34108230324', 678716528640):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25390589.eb28660136.56673.2071233912', 106767237120):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27243380.eb28501582.56608.36360891204', 280612730880):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27261805.eb28527427.56613.350237743056', 732933529600):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27260261.eb28612383.56663.255257743054', 222836459520):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27262884.eb28587751.56646.289167407405', 303644446720):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27248272.eb28426877.56596.40353030093', 240949913600):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25390589.eb28661741.56676.19963704861', 106764318720):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25390203.eb28661742.56676.28277457176', 107599400960):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25390203.eb28661509.56674.194020532406', 107595427840):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25386827.eb28563416.56632.30870270833', 151899863040):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27262884.eb28597392.56656.24319659722', 302931742720):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25387671.eb28616143.56665.27054978009', 107593492480):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25387671.eb28662252.56678.178276527775', 175879915520):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27264735.eb28603800.56659.224627627314', 151148257280):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25390589.eb28661624.56675.21209193287', 107598397440):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27261805.eb28549602.56618.334173599535', 369853593600):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27258103.eb28527619.56614.34754607639', 732399667200):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27248272.eb28094627.56590.41308579861', 240982169600):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27243380.eb28595272.56653.24096560185', 303101184000):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27243380.eb28586050.56643.330626863426', 302212106240):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27260261.eb28577580.56638.28133952546', 240500684800):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25386827.eb28575527.56636.32553855324', 345014323200):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25386827.eb28551343.56619.33367407408', 734067056640):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27259182.eb28586433.56644.321018194445', 601386250240):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb28624226.eb28625769.56669.43262586805', 46726881280):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27264735.eb28612294.56662.21643457176', 163865313280):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27243380.eb28499779.56605.3831562037', 303371038720):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25390589.eb28661773.56677.175470648144', 122865899520):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27263963.eb28581170.56639.31044631945', 222677882880):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27258103.eb28554755.56622.32566427083', 372400752640):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27243380.eb28590842.56647.268037835645', 303097466880):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb25390589.eb28636346.56672.1890915625', 107370936320):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27260261.eb28597220.56655.44330324074', 240395325440):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27258103.eb28527118.56612.36334712963', 732952104960):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27264735.eb28599207.56657.22997615741', 163699005440):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27261805.eb28559690.56629.30657424768', 344513710080):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27258103.eb28547464.56617.33934710648', 370022543360):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)],
            MeasurementSetData('13B-266.sb27260261.eb28612384.56663.4214728588', 222858373120):
                [FrequencyPair(940, 948), FrequencyPair(948, 956), FrequencyPair(956, 964), FrequencyPair(964, 972), FrequencyPair(972, 980), FrequencyPair(980, 988), FrequencyPair(988, 996),
                 FrequencyPair(996, 1004), FrequencyPair(1004, 1012), FrequencyPair(1012, 1020), FrequencyPair(1020, 1028), FrequencyPair(1028, 1036), FrequencyPair(1036, 1044),
                 FrequencyPair(1044, 1052), FrequencyPair(1052, 1060), FrequencyPair(1060, 1068), FrequencyPair(1068, 1076), FrequencyPair(1076, 1084), FrequencyPair(1084, 1092),
                 FrequencyPair(1092, 1100), FrequencyPair(1100, 1108), FrequencyPair(1108, 1116), FrequencyPair(1116, 1124), FrequencyPair(1124, 1132), FrequencyPair(1132, 1140),
                 FrequencyPair(1140, 1148), FrequencyPair(1148, 1156), FrequencyPair(1156, 1164), FrequencyPair(1164, 1172), FrequencyPair(1172, 1180), FrequencyPair(1180, 1188),
                 FrequencyPair(1188, 1196), FrequencyPair(1196, 1204), FrequencyPair(1204, 1212), FrequencyPair(1212, 1220), FrequencyPair(1220, 1228), FrequencyPair(1228, 1236),
                 FrequencyPair(1236, 1244), FrequencyPair(1244, 1252), FrequencyPair(1252, 1260), FrequencyPair(1260, 1268), FrequencyPair(1268, 1276), FrequencyPair(1276, 1284),
                 FrequencyPair(1284, 1292), FrequencyPair(1292, 1300), FrequencyPair(1300, 1308), FrequencyPair(1308, 1316), FrequencyPair(1316, 1324), FrequencyPair(1324, 1332),
                 FrequencyPair(1332, 1340), FrequencyPair(1340, 1348), FrequencyPair(1348, 1356), FrequencyPair(1356, 1364), FrequencyPair(1364, 1372), FrequencyPair(1372, 1380),
                 FrequencyPair(1380, 1388), FrequencyPair(1388, 1396), FrequencyPair(1396, 1404), FrequencyPair(1404, 1412), FrequencyPair(1412, 1420), FrequencyPair(1420, 1428)]}

    def test_build_graph01(self):
        reported_running = {
            'i2.4xlarge': [{'instance_type': 'i2.4xlarge', 'ip_address': '127.0.0.1', 'uuid': '33e050cd-3bbb-4396-b32f-072fc69c069b'},
                           {'instance_type': 'i2.4xlarge', 'ip_address': '127.0.0.2', 'uuid': '33e050cd-3bbb-4396-b32f-072fc69c069b'}],
            'i2.2xlarge': [{'instance_type': 'i2.2xlarge', 'ip_address': '128.0.0.1', 'uuid': '33e050cd-3bbb-4396-b32f-072fc69c069b'},
                           {'instance_type': 'i2.2xlarge', 'ip_address': '128.0.0.2', 'uuid': '33e050cd-3bbb-4396-b32f-072fc69c069b'},
                           {'instance_type': 'i2.2xlarge', 'ip_address': '128.0.0.3', 'uuid': '33e050cd-3bbb-4396-b32f-072fc69c069b'},
                           {'instance_type': 'i2.2xlarge', 'ip_address': '128.0.0.4', 'uuid': '33e050cd-3bbb-4396-b32f-072fc69c069b'},
                           {'instance_type': 'i2.2xlarge', 'ip_address': '128.0.0.5', 'uuid': '33e050cd-3bbb-4396-b32f-072fc69c069b'},
                           {'instance_type': 'i2.2xlarge', 'ip_address': '128.0.0.6', 'uuid': '33e050cd-3bbb-4396-b32f-072fc69c069b'}]
        }
        graph = BuildGraph(
                self.work_to_do,
                'bucket_name',
                'volume',
                7,
                reported_running,
                True,
                8)
        graph.build_graph()
        map_day_to_node = graph.map_day_to_node
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25390589', 107370936320)], '128.0.0.4')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25387671.e', 107599503360)], '128.0.0.5')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27264735.', 163699005440)], '128.0.0.4')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27248272.', 240982169600)], '128.0.0.6')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27243380.e', 303097466880)], '128.0.0.5')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27261805.e', 369853593600)], '128.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27262884.e', 303644446720)], '128.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25386827.', 151899863040)], '128.0.0.6')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27258103.', 370022543360)], '128.0.0.3')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27258103.', 678716528640)], '127.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25386827.', 345014323200)], '128.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27260261.', 240500684800)], '128.0.0.4')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25387671.e', 175879915520)], '128.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25386827.', 734067056640)], '127.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27248272.', 240949913600)], '128.0.0.3')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27260261', 222858373120)], '128.0.0.4')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25390589.', 107598397440)], '128.0.0.5')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27258103.', 372400752640)], '128.0.0.6')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25387671.', 107593492480)], '128.0.0.5')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27261805.', 344513710080)], '128.0.0.6')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27262884.', 302931742720)], '128.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb28624226.', 46726881280)], '128.0.0.3')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27259182.e', 601386250240)], '127.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27261805.e', 732933529600)], '127.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25390203.', 107599400960)], '128.0.0.3')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25390589', 106767237120)], '127.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27258103.', 732399667200)], '127.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27260261.e', 222836459520)], '128.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27260261.', 240395325440)], '128.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25390203.e', 107595427840)], '128.0.0.3')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27243380.e', 302212106240)], '127.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27264735.', 335832197120)], '127.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27264735.e', 151148257280)], '128.0.0.4')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27261033.', 175966218240)], '128.0.0.6')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25390589.e', 122865899520)], '128.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27243380', 303371038720)], '128.0.0.5')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27264735.', 163865313280)], '128.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27261805.', 369835100160)], '128.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27258103.', 732952104960)], '127.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27243380.', 280612730880)], '128.0.0.1')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27243380.', 303101184000)], '127.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb27263963.', 222677882880)], '128.0.0.2')
        self.assertEqual(map_day_to_node[MeasurementSetData('13B-266.sb25390589.', 106764318720)], '128.0.0.3')


if __name__ == '__main__':
    unittest.main()
