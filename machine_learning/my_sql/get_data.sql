USE `chiles02`;
DROP procedure IF EXISTS `get_data`;

DELIMITER $$
USE `chiles02`$$
CREATE DEFINER=`root`@`%` PROCEDURE `get_data`()
  BEGIN
    DECLARE frequency INT DEFAULT 944;
    DECLARE filename VARCHAR(250);
    DECLARE sqlstatement TEXT DEFAULT NULL;
    WHILE frequency <= 1416 DO
      SET filename = CONCAT('/tmp/min_freq_', frequency, '.csv');

      SET @sqlquery = CONCAT(
          ' SELECT * FROM (',
          '   SELECT \'scan\', \'channel\', \'name\', \'rms\', \'median\', \'max\', \'min\', \'mean\', \'stddev\'',
          '   UNION ALL',
          '   (',
          '     SELECT scan, channel, name, rms, median, max, min, mean, stddev',
          '     FROM visstat',
          '     inner join measurement_set on measurement_set.measurement_set_id = visstat.measurement_set_id',
          '     inner join day_name on day_name.day_name_id = measurement_set.day_name_id',
          '     WHERE min_frequency = ', frequency,
          '     ORDER BY scan, channel',
          '   )',
          ' ) result_set',
          ' INTO OUTFILE \'',filename, '\' FIELDS TERMINATED BY \',\' LINES TERMINATED BY \'\n\' ');
      PREPARE sqlstatement FROM @sqlquery;
      EXECUTE sqlstatement;

      SET frequency = frequency + 4;
    END WHILE;
  END$$

DELIMITER ;

