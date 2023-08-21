/****** Object:  StoredProcedure [audit_mig].[spBatchInsertUpdate]    Script Date: 21/08/2023 16:23:46 ******/
/****** Object:  StoredProcedure [audit_mig].[spadhocTrustedToUnified]    Script Date: 21/08/2023 16:42:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/********************************************************************************************
Procedure:          [audit_mig].[spBatchInsertUpdate]
Create Date:         
Author:             Premnath Ghawalkar
Description:        This sp ingest data from raw to foundation query.
                     
                     
Call by:            [audit_mig].[spBatchInsertUpdate]
                    [Azure Data Factory]
 
Affected table(s):  [audit_mig].[spBatchInsertUpdate]
                     
Used By:            
Parameter(s):       None
                    
Usage:              DECLARE @RC int
					DECLARE @sitecode varchar(10)
					DECLARE @env varchar(10)
					DECLARE @results varchar(max)

					-- TODO: Set parameter values here.

					EXECUTE @RC = [audit_mig].[spTrustedToUnified] 
					   'UK'
					  ,'dev'
					  ,@results OUTPUT
					GO
***********************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ---------------- ------------------------------------------------------------
 XXXX     Premnath Ghawalkar  Initial Version

***************************************************************************************************/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




-- Main Procedure Create.
CREATE  OR REPLACE    PROCEDURE [audit_mig].[spBatchInsertUpdate]
(
    @p_flag  varchar(100)  ,
	@p_sitecode varchar(10),
    @p_Output bit  OUTPUT
)
AS

BEGIN
    IF (@p_flag='START')

/* Insert a new entry into Batch table for All other process except 'SRC-FULL-BACKUP'  and 'ADHOC-BY-CLINIC-UNIFIED', by referencing details from dataingestionprocessdetails  Table. */
 
 BEGIN
  BEGIN TRY
      INSERT INTO audit_mig.batch
      SELECT a.batch_id,
a.processid,
a.process_name,
a.process_start_datetime ,
a.process_end_datetime ,
a.process_status ,
a.batch_start_datetime,
a.batch_end_datetime,
batch_status
FROM   (  SELECT
(
SELECT Max(batch_id+1) as Batch_id
FROM   audit_mig.batch where Batch_ID in (
select Batch_ID from  audit_mig.batch where batch_id=(select max(batch_id) from audit_mig.batch ) and Batch_Status='Completed' and Process_Status='Completed' )
) AS Batch_ID ,
processid ,
processname AS Process_Name ,
CASE
WHEN processname in ('OTHRS-SRC-TO-RAW','APPNT-SRC-TO-RAW','PANSR-SRC-TO-RAW')  THEN   SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'    
ELSE NULL
END [Process_Start_DateTime] ,
NULL [Process_End_DateTime] ,
NULL [Process_Status] ,
SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  [Batch_Start_DateTime] ,
NULL [Batch_End_DateTime] ,
'Initiated' [Batch_Status]
FROM   audit_mig.dataingestionprocessdetails
WHERE   SiteCode= @p_sitecode and ProcessShortDesc is null and ActiveFlag='1' and ProcessSchemaName NOT IN ('SRC-FULL-BACKUP','ADHOC-BY-CLINIC-UNIFIED','SNSACTION_BACKUP' )) a where  a.Batch_id  is not null ;

IF @@Rowcount =0
 SET @p_Output = 0
ELSE
 SET @p_Output = 1


	END TRY	  
	BEGIN CATCH
	SET @p_Output = 0
	 END CATCH
	
    END
	
	ELSE
	IF (@p_flag='START_FULLBACKUP')

/* Insert a new entry into Batch table for 'SRC-FULL-BACKUP' Process ONLY, by referencing details from dataingestionprocessdetails  Table. */
 
 BEGIN
  BEGIN TRY
      INSERT INTO audit_mig.batch
      SELECT a.batch_id,
a.processid,
a.process_name,
a.process_start_datetime ,
a.process_end_datetime ,
a.process_status ,
a.batch_start_datetime,
a.batch_end_datetime,
batch_status
FROM   (  SELECT
(
SELECT Max(batch_id+1) as Batch_id
FROM   audit_mig.batch where Batch_ID in (
select Batch_ID from  audit_mig.batch where batch_id=(select max(batch_id) from audit_mig.batch ) and Batch_Status='Completed' and Process_Status='Completed' )
) AS Batch_ID ,
processid ,
processname AS Process_Name ,
CASE
WHEN processname = 'SRC-FULL-BACKUP'  THEN   SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'    
ELSE NULL
END [Process_Start_DateTime] ,
NULL [Process_End_DateTime] ,
NULL [Process_Status] ,
SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  [Batch_Start_DateTime] ,
NULL [Batch_End_DateTime] ,
'Initiated' [Batch_Status]
FROM   audit_mig.dataingestionprocessdetails
WHERE   SiteCode= @p_sitecode and ProcessShortDesc is null and ActiveFlag='1' and ProcessSchemaName = 'SRC-FULL-BACKUP' ) a where  a.Batch_id  is not null ;

IF @@Rowcount =0
 SET @p_Output = 0
ELSE
 SET @p_Output = 1


	END TRY	  
	BEGIN CATCH
	SET @p_Output = 0
	 END CATCH
	
    END

	ELSE
	IF (@p_flag='START_SNSACTION_BACKUP')

/* Insert a new entry into Batch table for 'START_SNSACTION_BACKUP' Process ONLY, by referencing details from dataingestionprocessdetails  Table. */
 
 BEGIN
  BEGIN TRY
      INSERT INTO audit_mig.batch
      SELECT a.batch_id,
a.processid,
a.process_name,
a.process_start_datetime ,
a.process_end_datetime ,
a.process_status ,
a.batch_start_datetime,
a.batch_end_datetime,
batch_status
FROM   (  SELECT
(
SELECT Max(batch_id+1) as Batch_id
FROM   audit_mig.batch where Batch_ID in (
select Batch_ID from  audit_mig.batch where batch_id=(select max(batch_id) from audit_mig.batch ) and Batch_Status='Completed' and Process_Status='Completed' )
) AS Batch_ID ,
processid ,
processname AS Process_Name ,
CASE
WHEN processname = 'SNSACTION_BACKUP'  THEN   SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'    
ELSE NULL
END [Process_Start_DateTime] ,
NULL [Process_End_DateTime] ,
NULL [Process_Status] ,
SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  [Batch_Start_DateTime] ,
NULL [Batch_End_DateTime] ,
'Initiated' [Batch_Status]
FROM   audit_mig.dataingestionprocessdetails
WHERE   SiteCode= @p_sitecode and ProcessShortDesc is null and ActiveFlag='1' and ProcessSchemaName = 'SNSACTION_BACKUP' ) a where  a.Batch_id  is not null ;

IF @@Rowcount =0
 SET @p_Output = 0
ELSE
 SET @p_Output = 1


	END TRY	  
	BEGIN CATCH
	SET @p_Output = 0
	 END CATCH
	
    END

	ELSE
	IF (@p_flag='START_ADHOC_BY_CLINIC')
	/* V4.0 Changes - Insert a new entry into Batch table for 'ADHOC-BY-CLINIC-UNIFIED' Process ONLY, by referencing details from dataingestionprocessdetails  Table. */
 
 BEGIN
  BEGIN TRY
      INSERT INTO audit_mig.batch
      SELECT a.batch_id,
a.processid,
a.process_name,
a.process_start_datetime ,
a.process_end_datetime ,
a.process_status ,
a.batch_start_datetime,
a.batch_end_datetime,
batch_status
FROM   (  SELECT
(
SELECT Max(batch_id+1) as Batch_id
FROM   audit_mig.batch where Batch_ID in (
select Batch_ID from  audit_mig.batch where batch_id=(select max(batch_id) from audit_mig.batch ) and Batch_Status='Completed' and Process_Status='Completed' )
) AS Batch_ID ,
processid ,
processname AS Process_Name ,
CASE
WHEN processname = 'ADHOC-BY-CLINIC-UNIFIED'  THEN   SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'    
ELSE NULL
END [Process_Start_DateTime] ,
NULL [Process_End_DateTime] ,
NULL [Process_Status] ,
SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  [Batch_Start_DateTime] ,
NULL [Batch_End_DateTime] ,
'Initiated' [Batch_Status]
FROM   audit_mig.dataingestionprocessdetails
WHERE   SiteCode= @p_sitecode and ProcessShortDesc is null and ActiveFlag='1' and ProcessSchemaName = 'ADHOC-BY-CLINIC-UNIFIED' ) a where  a.Batch_id  is not null ;

IF @@Rowcount =0
 SET @p_Output = 0
ELSE
 SET @p_Output = 1


	END TRY	  
	BEGIN CATCH
	SET @p_Output = 0
	 END CATCH
	
    END

	
    ELSE
    IF (@p_flag='RAW_RUN')
/* 	When p_flag= 'RAW_RUN' AND process_name='SRC-TO-RAW' AND batch_status='Initiated', Update audit_mig.batch table with process_status='Running',batch_status='Running'. */

    BEGIN
      UPDATE audit_mig.batch
      SET    process_status='Running',
             batch_status='Running' ,
             process_start_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  process_name='SRC-TO-RAW'
      AND    batch_status='Initiated'
      SET    @p_Output = 1
    END
    ELSE
    IF (@p_flag='RAW_COMPLETE')
/* 	When p_flag= 'RAW_COMPLETE' AND process_name='SRC-TO-RAW' AND batch_status='Running', Update audit_mig.batch table with process_status='Completed'. */
	
    BEGIN
      UPDATE audit_mig.batch
      SET    process_end_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  ,
             process_status='Completed'
      WHERE  process_name='SRC-TO-RAW'
      AND    batch_status='Running'
      SET    @p_Output = 1
    END
    ELSE
    IF (@p_flag='FND_RUN')
/* 	When p_flag= 'FND_RUN' AND  process_name='RAW-TO-FOUNDATION' AND batch_status='Initiated', Update audit_mig.batch table with process_status='Running',batch_status='Running'. */
	
    BEGIN
      UPDATE audit_mig.batch
      SET    process_status='Running',
             batch_status='Running' ,
             process_start_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  process_name='RAW-TO-FOUNDATION'
      AND    batch_status='Initiated'
      SET    @p_Output =  1
    END
    ELSE
    IF (@p_flag='FND_COMPLETE')
/* 	When p_flag= 'FND_COMPLETE' AND process_name='RAW-TO-FOUNDATION' AND batch_status='Running', Update audit_mig.batch table with process_status='Completed'. */

    BEGIN
      UPDATE audit_mig.batch
      SET    process_end_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  ,
             process_status='Completed'
      WHERE  process_name='RAW-TO-FOUNDATION'
      AND    batch_status='Running'
      SET    @p_Output = 1
    END
    ELSE
    IF (@p_flag='TRST_RUN')
/* 	When p_flag= 'TRST_RUN' AND process_name='FOUNDATION-TO-TRUSTED' AND batch_status='Initiated' , Update audit_mig.batch table with process_status='Running',batch_status='Running'. */
	
    BEGIN
      UPDATE audit_mig.batch
      SET    process_status='Running' ,
             batch_status='Running',
             process_start_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  process_name='FOUNDATION-TO-TRUSTED'
      AND    batch_status='Initiated'
      SET    @p_Output = 1
    END
    ELSE
    IF (@p_flag='TRST_COMPLETE')
/* 	When p_flag= 'TRST_COMPLETE' AND process_name='FOUNDATION-TO-TRUSTED' AND batch_status='Running', Update audit_mig.batch table with process_status='Completed' */
/*  Also updated batch_status = 'Completed' for any batch_status = 'Running' */
	
    BEGIN
      UPDATE audit_mig.batch
      SET    process_status='Completed' ,
             process_end_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  process_name='FOUNDATION-TO-TRUSTED'
      AND    batch_status='Running'
      UPDATE audit_mig.batch
      SET    batch_status='Completed' ,
             batch_end_datetime =SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  batch_status='Running'
      SET    @p_Output = 1
    END
    ELSE
	 IF (@p_flag='UNIFIED_RUN')
/* 	When p_flag= 'UNIFIED_RUN', AND process_name='TRUSTED-TO-UNIFIED' AND batch_status='Initiated',  Update audit_mig.batch table with process_status='Running',batch_status='Running' */	 

    BEGIN
      UPDATE audit_mig.batch
      SET    process_status='Running',
             batch_status='Running' ,
             process_start_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  process_name='TRUSTED-TO-UNIFIED'
      AND    batch_status='Initiated'
      SET    @p_Output = 1
    END
	ELSE
	IF (@p_flag='UNIFIED_COMPLETE')
/* 	When p_flag= 'UNIFIED_COMPLETE' AND process_name='TRUSTED-TO-UNIFIED' AND batch_status='Running' , Update audit_mig.batch table with process_status='Completed'.	*/
/* 	Also updated batch_status = 'Completed' for any batch_status = 'Running' */

    BEGIN
      UPDATE audit_mig.batch
      SET    process_status='Completed' ,
             process_end_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  process_name='TRUSTED-TO-UNIFIED'
      AND    batch_status='Running'
      UPDATE audit_mig.batch
      SET    batch_status='Completed' ,
             batch_end_datetime =SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  batch_status='Running'
      SET    @p_Output = 1
    END
    ELSE
    IF (@p_flag='FULLBACKUP_RUN')
/* 	When p_flag= 'FULLBACKUP_RUN' AND process_name='SRC-FULL-BACKUP' AND batch_status='Initiated', Update audit_mig.batch table with process_status='Running',batch_status='Running'. */

    BEGIN
      UPDATE audit_mig.batch
      SET    process_status='Running',
             batch_status='Running' ,
             process_start_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  process_name='SRC-FULL-BACKUP'
      AND    batch_status='Initiated'
      SET    @p_Output = 1
    END
    ELSE
    IF (@p_flag='FULLBACKUP_COMPLETE')
/* 	When p_flag= 'FULLBACKUP_COMPLETE' AND process_name='SRC-FULL-BACKUP' AND batch_status='Running', Update audit_mig.batch table with process_status='Completed'. */
	
    BEGIN
      UPDATE audit_mig.batch
      SET    process_end_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  ,
			 batch_end_datetime =SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  , 
             process_status='Completed',batch_status='Completed'
      WHERE  process_name='SRC-FULL-BACKUP'
      AND    batch_status='Running'
      SET    @p_Output = 1
    END

    ELSE
    IF (@p_flag='SNSACTION_BACKUP_RUN')
/* 	When p_flag= 'SNSACTION_BACKUP_RUN' AND process_name='SNSACTION_BACKUP' AND batch_status='Initiated', Update audit_mig.batch table with process_status='Running',batch_status='Running'. */

    BEGIN
      UPDATE audit_mig.batch
      SET    process_status='Running',
             batch_status='Running' ,
             process_start_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  process_name='SNSACTION_BACKUP'
      AND    batch_status='Initiated'
      SET    @p_Output = 1
    END
    ELSE
    IF (@p_flag='SNSACTION_BACKUP_COMPLETE')
/* 	When p_flag= 'SNSACTION_BACKUP_COMPLETE' AND process_name='SNSACTION_BACKUP' AND batch_status='Running', Update audit_mig.batch table with process_status='Completed'. */
	
    BEGIN
      UPDATE audit_mig.batch
      SET    process_end_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  ,
			 batch_end_datetime =SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  , 
             process_status='Completed',batch_status='Completed'
      WHERE  process_name='SNSACTION_BACKUP'
      AND    batch_status='Running'
      SET    @p_Output = 1
    END

    ELSE
    IF (@p_flag='ADHOC_BY_CLINIC_RUN')
/* 	V4.0 Changes - When p_flag= 'ADHOC_BY_CLINIC_RUN' AND process_name='ADHOC-BY-CLINIC-UNIFIED' AND batch_status='Initiated', Update audit_mig.batch table with process_status='Running',batch_status='Running'. */

    BEGIN
      UPDATE audit_mig.batch
      SET    process_status='Running',
             batch_status='Running' ,
             process_start_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time' 
      WHERE  process_name='ADHOC-BY-CLINIC-UNIFIED'
      AND    batch_status='Initiated'
      SET    @p_Output = 1
    END
    ELSE
    IF (@p_flag='ADHOC_BY_CLINIC_COMPLETE')
/* 	V4.0 Changes - When p_flag= 'ADHOC_BY_CLINIC_COMPLETE' AND process_name='ADHOC-BY-CLINIC-UNIFIED' AND batch_status='Running', Update audit_mig.batch table with process_status='Completed'. */
	
    BEGIN
      UPDATE audit_mig.batch
      SET    process_end_datetime=SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  ,
			 batch_end_datetime =SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  , 
             process_status='Completed',batch_status='Completed'
      WHERE  process_name='ADHOC-BY-CLINIC-UNIFIED'
      AND    batch_status='Running'
      SET    @p_Output = 1
    END
	
	ELSE

    BEGIN
      SET @p_Output = 0
    END
  END;
GO


