/****** Object:  StoredProcedure [audit_mig].[spFundToTrusted]    Script Date: 21/08/2023 17:02:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE OR REPLACE  PROCEDURE[audit_mig].[spFundToTrusted] (
    @sitecode varchar(10),
	@env varchar(10),
	@LoadType varchar(10),
    @results varchar(max) OUTPUT ) AS
BEGIN

IF @env='dev' 

BEGIN
		SELECT  [ID]
		, CAST(b.[Batch_ID] as varchar) as Batch_ID
		, cast(b.[Batch_Start_DateTime] as datetime) as Batch_Start_DateTime
		,CONCAT(DestinationSchema ,'_d') as [FoundationSchema]
		, [SourceSchema]  as [p_FoundationSchema]
		,[SourceSchema] as [SourceSchema]
		,[SourceTable]
		,[SourceSecretKey] as Country
		, [SourceUserDefinesql]  as    [SourceSQL] 
		,[StorageContainter]
		,CONCAT('t_',DestinationSchema,'_d') AS  [TrustedSchema]
		,[DestinationTable] AS TrustedTable
		,[Active_Flag]
		,[Archival_Flag]
		,[GroupID]
		,[OrderID]
		--, CASE  WHEN [LoadType] = 'F'  THEN 'Full' WHEN  [LoadType] = 'D'  THEN 'Delta' ELSE 'N/A'  END as  [LoadType]
		, Case when @LoadType='F' then 'Full' else 'Delta' end  as LoadType
		,[SiteCode]
		FROM  [audit_mig].[adf_datapipeline_config]  t  JOIN [audit_mig].[Batch] b
		ON t.Source_Flag=b.Process_Name and b.Batch_Status='Running'  
		where t.Source_Flag='FOUNDATION-TO-TRUSTED'    AND   
		t.Active_Flag=1 and 
		SiteCode=@sitecode
		ORDER BY [OrderID]
END

ELSE IF @env='test'


BEGIN
		SELECT  [ID]
		, CAST(b.[Batch_ID] as varchar) as Batch_ID
		, cast(b.[Batch_Start_DateTime] as datetime) as Batch_Start_DateTime
		,CONCAT(DestinationSchema ,'_t') as [FoundationSchema]
			, [SourceSchema]  as [p_FoundationSchema]
		,[SourceSchema] as [SourceSchema]
		,[SourceTable]
		,[SourceSecretKey] as Country
		, [SourceUserDefinesql]  as    [SourceSQL] 
		,[StorageContainter]
		,CONCAT('t_',DestinationSchema,'_t') AS  [TrustedSchema]
		,[DestinationTable] AS TrustedTable
		,[Active_Flag]
		,[Archival_Flag]
		,[GroupID]
		,[OrderID]
		--, CASE  WHEN [LoadType] = 'F'  THEN 'Full' WHEN  [LoadType] = 'D'  THEN 'Delta' ELSE 'N/A'  END as  [LoadType]
		, Case when @LoadType='F' then 'Full' else 'Delta' end  as LoadType
		,[SiteCode]
		FROM  [audit_mig].[adf_datapipeline_config]  t  JOIN [audit_mig].[Batch] b
		ON t.Source_Flag=b.Process_Name and b.Batch_Status='Running'  
		where t.Source_Flag='FOUNDATION-TO-TRUSTED'    AND   
		t.Active_Flag=1 and 
		SiteCode=@sitecode
		ORDER BY [OrderID]
END

ELSE

BEGIN
		SELECT  [ID]
		, CAST(b.[Batch_ID] as varchar) as Batch_ID
		, cast(b.[Batch_Start_DateTime] as datetime) as Batch_Start_DateTime
		,DestinationSchema as [FoundationSchema]
				,[SourceSchema]  as [p_FoundationSchema]
		,[SourceSchema] as [SourceSchema]
		,[SourceTable]
		,[SourceSecretKey] as Country
		, [SourceUserDefinesql]  as    [SourceSQL] 
		---, replace([SourceUserDefinesql],'dev','test') as [SourceSQL] 
		,[StorageContainter]
		,CONCAT('t_',DestinationSchema) AS  [TrustedSchema]
		,[DestinationTable] AS TrustedTable
		,[Active_Flag]
		,[Archival_Flag]
		,[GroupID]
		--, CASE  WHEN [LoadType] = 'F'  THEN 'Full' WHEN  [LoadType] = 'D'  THEN 'Delta' ELSE 'N/A'  END as  [LoadType]
		, Case when @LoadType='F' then 'Full' else 'Delta' end  as LoadType
		,[OrderID]
		,[SiteCode]
		FROM  [audit_mig].[adf_datapipeline_config]  t  JOIN [audit_mig].[Batch] b
		ON t.Source_Flag=b.Process_Name and b.Batch_Status='Running'  
		where t.Source_Flag='FOUNDATION-TO-TRUSTED'    AND   t.Active_Flag=1 and 
		SiteCode=@sitecode
		ORDER BY [OrderID]
END


	Return;
	
END
GO


