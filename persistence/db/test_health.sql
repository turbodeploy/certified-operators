DELIMITER // 
DROP PROCEDURE IF EXISTS PopulateAugustInfo//
CREATE PROCEDURE PopulateAugustInfo() 
BEGIN 

  DECLARE done INT DEFAULT 0;

  /*
   * Get tables variables
   */
  DECLARE Table_in_vmtdb VARCHAR(80);
  DECLARE Table_type VARCHAR(16);

  /*
   * Check table variables
   */
  DECLARE Check_table VARCHAR(80);
  DECLARE Check_operation VARCHAR(80);
  DECLARE Check_Msg_type VARCHAR(80);
  DECLARE Check_Msg_text TEXT;

  /*
   * Init for loop:
   */
  DECLARE cur1 CURSOR FOR SHOW FULL TABLES IN vmtdb WHERE Table_type='BASE TABLE';
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
  OPEN cur1;

  /*
   * Loop:
   */
  read_loop: LOOP
    FETCH cur1 INTO Table_in_vmtdb, Table_type;
    IF done THEN
      LEAVE read_loop;
    END IF;
    
    
    
  END LOOP;

  /*
   * Clean up
   */
  CLOSE cur1;


END // 
DELIMITER ; 