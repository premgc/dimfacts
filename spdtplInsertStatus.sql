/****** Object:  StoredProcedure [audit_mig].[spdtplInsertStatus]    Script Date:   ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






/****** Object:  StoredProcedure [audit_mig].[spRunGenericSQLStatement]    Script Date: 12/10/2020 09:47:31 ******/

/***************************************************************************************************
Procedure:          [audit_mig].[spdtplInsertStatus] 
Create Date:        
Author:             Premnath Ghawalkar
Description:        This procedure executes dynamic SQL statement pass as a parameter @sqlStmt
                     
                     
Call by:            [audit_mig].[spRunGenericSQLStatement]
                    [Azure Data Factory]
 
Affected table(s):  [audit_mig].[spdtplInsertStatus]
                     
Used By:            
Parameter(s):       None
                    
Usage:              exec [audit_mig].[spdtplInsertStatus] [@ProcessName] [@ProcessFlag] [@ProcessSchemaName] [@ProcessStatus] 
                    Additional notes or caveats about this object, like where is can and cannot be run, or
                    gotchas to watch for when using it.
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
				Premnath Ghawalkar  Initial Version
				Premnath Ghawalkar  added @SiteCode 
***************************************************************************************************/

CREATE              PROCEDURE [audit_mig].[spdtplInsertStatus] (
@ProcessName varchar(50),
@AdfPipeLineRunid varchar(50),
@DataFactoryName varchar(50),
@SiteCode varchar(10)
)
AS 
BEGIN

INSERT INTO  [audit_mig].[AdfPipelineExecutionLog] ([AdfPipelineRunId]  ,[ProcessId] ,[Batch_ID]  ,[DataFactoryName]     ,[PipelineName]     ,[StartDateTime]     ,[PipelineStatus] ,[SourceTable] ,[DestinationTable]   ,[EndDateTime], [LoadType]) 
SELECT  DISTINCT
 @AdfPipeLineRunid AdfPipeLineRunid,  --'4010e094-627d-4189-9a4d-0f64e66df131' @AdfPipeLineRunid
ProcessID, 
d.Batch_ID,
@DataFactoryName   DataFactoryName , --'07efdaasadf01' @DataFactoryName 
 a.Pipeline, 
SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  as StartDateTime,
'Initiated'  as PipelineStatus, 
 concat(lower(b.SourceSchema),'.',lower(b.SourceTable)) as SourceTable,
 concat(lower(b.DestinationSchema),'.', lower(b.DestinationTable)) as DestinationTable,
NULL as EndDateTime ,
CASE 
         WHEN b.[loadtype] = 'D' AND incrementalrefkey IS NOT NULL AND c.value IS NOT NULL THEN 
		 'Delta Load'
		 WHEN ProcessShortDesc like '%SRC-TO-RAW' and b.[loadtype] is null OR incrementalrefkey is null OR  c.value is null THEN 
		 'Full Load'
		WHEN ProcessShortDesc like '%SRC-TO-RAW' and b.[loadtype] is not null OR incrementalrefkey is not null OR  c.value is null THEN 
		 'Full Load'
		 else NULL
End as LoadType
from [audit_mig].[DataIngestionProcessDetails] a INNER join [audit_mig].[adf_datapipeline_config] b
ON a.ProcessSchemaName=b.SourceSchema and a.ProcessName=b.SourceTable
LEFT JOIN  audit_mig.watermark_lastrunmetadata c ON b.IncrementalRefKey=c.[key] 
LEFT JOIN (select DISTINCT  Batch_ID FROM [audit_mig].[Batch] WHERE Batch_Status='Running'  ) d ON 1=1
WHERE   
a.ActiveFlag='1' AND 
b.Active_Flag='1'  AND 
a.ProcessShortDesc=@ProcessName AND a.SiteCode=@SiteCode AND
 b.Source_Flag=@ProcessName order by ProcessID asc;
-- SELECT  DISTINCT
-- @AdfPipeLineRunid  AdfPipeLineRunid,  --'4010e094-627d-4189-9a4d-0f64e66df131'
-- ProcessID,  
-- @DataFactoryName  DataFactoryName , --'07efdaasadf01'
--  a.Pipeline, 
-- SYSDATETIMEOFFSET() AT TIME ZONE 'GMT Standard Time'  as StartDateTime,
-- 'Running' as PipelineStatus, 
--  concat(lower(b.SourceSchema),'.',lower(b.SourceTable)) as SourceTable,
--  concat(lower(b.DestinationSchema),'.', lower(b.DestinationTable)) as DestinationTable,
-- NULL as EndDateTime ,
-- CASE 
--          WHEN b.[loadtype] = 'D' AND incrementalrefkey IS NOT NULL AND c.value IS NOT NULL THEN 
-- 		 'Delta Load'
-- 		 WHEN ProcessShortDesc like '%SRC-TO-RAW' and b.[loadtype] is null OR incrementalrefkey is null OR  c.value is null THEN 
-- 		 'Full Load'
-- 		WHEN ProcessShortDesc like '%SRC-TO-RAW' and b.[loadtype] is not null OR incrementalrefkey is not null OR  c.value is null THEN 
-- 		 'Full Load'
-- 		 else NULL
-- End as LoadType
-- from [audit_mig].[DataIngestionProcessDetails] a INNER join [audit_mig].[adf_datapipeline_config] b
-- ON a.ProcessSchemaName=b.SourceSchema and a.ProcessName=b.SourceTable
-- LEFT JOIN  audit_mig.watermark_lastrunmetadata c ON b.IncrementalRefKey=c.[key]
-- WHERE   
-- a.ActiveFlag='1' AND 
-- b.Active_Flag='1'  AND 
-- a.ProcessShortDesc=@ProcessName AND 
--  b.Source_Flag=@ProcessName order by ProcessID asc;



END
GO


