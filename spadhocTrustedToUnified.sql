/****** Object:  StoredProcedure [audit_mig].[spadhocTrustedToUnified]    Script Date:   ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/********************************************************************************************
Procedure:          [audit_mig].[spadhocTrustedToUnified]
Create Date:         
Author:             Premnath Ghawalkar
Description:        This sp ingest data from raw to foundation query.
                     
                     
Call by:            [audit_mig].[spadhocTrustedToUnified]
                    [Azure Data Factory]
 
Affected table(s):  [audit_mig].[spadhocTrustedToUnified]
                     
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

CREATE  OR REPLACE    PROCEDURE [audit_mig].[spadhocTrustedToUnified] (
    @sitecode varchar(10),
	@env varchar(10),
	@LoadType varchar(10),
    @results varchar(max) OUTPUT ) AS
BEGIN


IF @env='dev' 

BEGIN

SELECT  [ID]
		--, CAST(b.[Batch_ID] as varchar) as Batch_ID
		--, cast(b.[Batch_Start_DateTime] as datetime) as Batch_Start_DateTime
		,CONCAT(SourceSchema,'_d') as [TrustedSchema]
		,CONCAT(SourceSchema,'') as [p_TrustedSchema]
		,[SourceTable] as [interfaceName]
		, concat(DestinationSchema,'_d') as  [UnifiedSchema]
		,[SourceSecretKey] as Country
		, case when [SourceUserDefinesql] is null then 'N/A' else [SourceUserDefinesql]  end as    [SourceSQL] 
		,[StorageContainter]
		, [DestinationTable] as tblName 
		,case when storageFilepath='/mnt/auditdata-uk/inbox/non_obfuscate/'  then 
		     concat('/mnt/migration/auditdata-uk-dev/inbox/non_obfuscate/','') 
		  else  CONCAT('/mnt/migration/auditdata-uk-dev/inbox/','') end  AS  [OutputFilePath]
		,[StorageFileName] AS [OutputFileName]
		,[Active_Flag]
		, case when [obfuscate_flag]=1 then 'Y' else 'N' end as flag
		, case when [obfuscate_flag]=1 then 'Y' else 'N' end as  obfuscate_flag 
		,[GroupID]
		,[OrderID]
		,[SiteCode]
        ,case when @LoadType='F' then 'Full' else 'Delta' end  as LoadType
				,cast(b.Batch_ID as varchar) Batch_ID	
						, b.Process_Start_DateTime as Batch_date
  FROM  [audit_mig].[adf_datapipeline_config]  t  JOIN [audit_mig].[Batch] b ON t.Source_Flag=b.Process_Name and b.Batch_Status='Running'  
   where t.Source_Flag='ADHOC-BY-CLINIC-UNIFIED'    AND   t.Active_Flag=1 order by t.OrderID;
	
END

ELSE IF @env='test'


BEGIN
SELECT  [ID]
		--, CAST(b.[Batch_ID] as varchar) as Batch_ID
		--, cast(b.[Batch_Start_DateTime] as datetime) as Batch_Start_DateTime
		,CONCAT(SourceSchema,'_t') as [TrustedSchema]
			,CONCAT(SourceSchema,'') as [p_TrustedSchema]
		,[SourceTable] as [interfaceName]
		, concat(DestinationSchema,'_t') as  [UnifiedSchema]
		,[SourceSecretKey] as Country
	   , case when [SourceUserDefinesql] is null then 'N/A' else [SourceUserDefinesql]  end as    [SourceSQL] 
		,[StorageContainter]
		, [DestinationTable] as tblName
		,case when storageFilepath='/mnt/auditdata-uk/inbox/non_obfuscate/'  then 
		     concat('/mnt/migration/auditdata-uk-dev/inbox/non_obfuscate/','') 
		  else  CONCAT('/mnt/migration/auditdata-uk-dev/inbox/','') end  AS  [OutputFilePath]
		,[StorageFileName] AS [OutputFileName]
		,[Active_Flag]
		, case when [obfuscate_flag]=1 then 'Y' else 'N' end as flag
		, case when [obfuscate_flag]=1 then 'Y' else 'N' end as  obfuscate_flag
		,[GroupID]
		,[OrderID]
		,[SiteCode]
    ,case when @LoadType='F'  then 'Full' else 'Delta' end  as LoadType
	,cast(b.Batch_ID as varchar) Batch_ID	
			, b.Process_Start_DateTime as Batch_date
  FROM  [audit_mig].[adf_datapipeline_config]  t  JOIN [audit_mig].[Batch] b ON t.Source_Flag=b.Process_Name and b.Batch_Status='Running'  
   where t.Source_Flag='ADHOC-BY-CLINIC-UNIFIED'    AND   t.Active_Flag=1 order by t.OrderID;
	
END

ELSE

BEGIN

SELECT  [ID]
		--, CAST(b.[Batch_ID] as varchar) as Batch_ID
		--, cast(b.[Batch_Start_DateTime] as datetime) as Batch_Start_DateTime
		,[SourceSchema] as [TrustedSchema]
			,[SourceSchema] as [p_TrustedSchema]
		,[SourceTable] as [interfaceName]
			,  (DestinationSchema) as  [UnifiedSchema]
			,[SourceSecretKey] as Country
		, case when [SourceUserDefinesql] is null then 'N/A' else [SourceUserDefinesql]  end as    [SourceSQL] 
		,[StorageContainter]
		, [DestinationTable] as tblName
		,storageFilepath AS  [OutputFilePath]
		,[StorageFileName] AS [OutputFileName]
		,[Active_Flag]
		, case when [obfuscate_flag]=1 then 'Y' else 'N' end as flag
		, case when [obfuscate_flag]=1 then 'Y' else 'N' end as  obfuscate_flag
		,[GroupID]
		,[OrderID]
		,[SiteCode]
    ,case when @LoadType='F'  then 'Full' else 'Delta' end  as LoadType
	,cast(b.Batch_ID as varchar) Batch_ID		
			, b.Process_Start_DateTime as Batch_date
  FROM  [audit_mig].[adf_datapipeline_config]  t  JOIN [audit_mig].[Batch] b ON t.Source_Flag=b.Process_Name and b.Batch_Status='Running'  
   where t.Source_Flag='ADHOC-BY-CLINIC-UNIFIED'    AND   t.Active_Flag=1 order by t.OrderID;
		
END


	Return;
	
END
GO


