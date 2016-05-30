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
List the parsing
"""
import unittest

from casa_code.parse_listobs import ParseListobs

FILE_TEXT = '''================================================================================
           MeasurementSet Name:  /mnt/hidata/chiles/final_products/20140120_941_2_FINAL_PRODUCTS/13B-266.sb25390589.eb28661773.56677.175470648144_calibrated_deepfield.ms      MS Version 2
================================================================================
   Observer: Jacqueline H. van Gorkom     Project: uid://evla/pdb/25320050
Observation: EVLA(26 antennas)
Data records: 2535000       Total integration time = 6088 seconds
   Observed from   20-Jan-2014/04:20:12.0   to   20-Jan-2014/06:01:40.0 (UTC)

Fields: 1
  ID   Code Name                RA               Decl           Epoch   SrcId      nRows
  0    NONE deepfield           10:01:24.000000 +02.21.00.00000 J2000   0        2535000
Spectral Windows:  (15 unique spectral windows and 1 unique polarization setups)
  SpwID  Name           #Chans   Frame   Ch0(MHz)  ChanWid(kHz)  TotBW(kHz) BBC Num  Corrs
  0      EVLA_L#A0C0#0    2048   TOPO     941.000        15.625     32000.0      12  RR  LL
  1      EVLA_L#A0C0#1    2048   TOPO     973.000        15.625     32000.0      12  RR  LL
  2      EVLA_L#A0C0#2    2048   TOPO    1005.000        15.625     32000.0      12  RR  LL
  3      EVLA_L#A0C0#3    2048   TOPO    1037.000        15.625     32000.0      12  RR  LL
  4      EVLA_L#A0C0#4    2048   TOPO    1069.000        15.625     32000.0      12  RR  LL
  5      EVLA_L#A0C0#5    2048   TOPO    1101.000        15.625     32000.0      12  RR  LL
  6      EVLA_L#A0C0#6    2048   TOPO    1133.000        15.625     32000.0      12  RR  LL
  7      EVLA_L#A0C0#7    2048   TOPO    1165.000        15.625     32000.0      12  RR  LL
  8      EVLA_L#A0C0#8    2048   TOPO    1197.000        15.625     32000.0      12  RR  LL
  9      EVLA_L#A0C0#9    2048   TOPO    1229.000        15.625     32000.0      12  RR  LL
  10     EVLA_L#A0C0#10   2048   TOPO    1261.000        15.625     32000.0      12  RR  LL
  11     EVLA_L#A0C0#11   2048   TOPO    1293.000        15.625     32000.0      12  RR  LL
  12     EVLA_L#A0C0#12   2048   TOPO    1325.000        15.625     32000.0      12  RR  LL
  13     EVLA_L#A0C0#13   2048   TOPO    1357.000        15.625     32000.0      12  RR  LL
  14     EVLA_L#A0C0#14   2048   TOPO    1389.000        15.625     32000.0      12  RR  LL
Antennas: 26 'name'='station'
   ID=   0-3: 'ea01'='N32', 'ea02'='N28', 'ea03'='E28', 'ea04'='E24',
   ID=   4-7: 'ea05'='W08', 'ea07'='N12', 'ea08'='N16', 'ea09'='W28',
   ID=  8-11: 'ea10'='E04', 'ea11'='W24', 'ea12'='E08', 'ea13'='W20',
   ID= 12-15: 'ea14'='N24', 'ea15'='E20', 'ea16'='N08', 'ea17'='E32',
   ID= 16-19: 'ea18'='E36', 'ea19'='W16', 'ea20'='N04', 'ea21'='E12',
   ID= 20-23: 'ea23'='W36', 'ea24'='W04', 'ea25'='W32', 'ea26'='W12',
   ID= 24-25: 'ea27'='N36', 'ea28'='E16'
'''


class TestParseListObs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open('/tmp/listobs.txt', mode='w') as write_file:
            write_file.write(FILE_TEXT)

    def test_parse_file(self):
        parse_listobs = ParseListobs('/tmp/listobs.txt')
        parse_listobs.parse()
        data = parse_listobs.get_data()

        self.assertEqual(data['MeasurementSet Name'], '/mnt/hidata/chiles/final_products/20140120_941_2_FINAL_PRODUCTS/13B-266.sb25390589.eb28661773.56677.175470648144_calibrated_deepfield.ms')
        self.assertEqual(data['Observer'], 'Jacqueline H. van Gorkom')
        self.assertEqual(data['Project'], 'uid://evla/pdb/25320050')
        self.assertEqual(data['Observation'], 'EVLA(26 antennas)')
        self.assertEqual(data['Project'], 'uid://evla/pdb/25320050')
        self.assertEqual(data['Bottom edge'], '941.000')
        fields = data['Fields']
        self.assertEqual(fields['Field count'], '1')
        self.assertEqual(fields['Fields'][0][0], '0')


if __name__ == '__main__':
    unittest.main()
