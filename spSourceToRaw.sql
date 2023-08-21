/****** Object:  StoredProcedure [audit_mig].[spRawToFnd]    Script Date: 21/08/2023 17:07:28 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE  OR REPLACE  PROCEDURE[audit_mig].[spRawToFnd] (
    @sitecode varchar(10),
	@env varchar(10),
	@LoadType varchar(10),
    @results varchar(max) OUTPUT ) AS
BEGIN

IF @env='dev' 

BEGIN
	SELECT  [ID],
	CAST(b.[Batch_ID] as varchar) as Batch_ID,
	cast(b.[Batch_Start_DateTime] as datetime) as Batch_Start_DateTime ,
	concat([SourceSchema],'_d') as [SourceSchema],
	concat([SourceSchema],'') as [p_SourceSchema],
	[SourceTable] , 
	---case when [LoadType]='F'  then  'N/A'  else [SourceUserDefinesql] end   as [Merge] ,
	case when [SourceUserDefinesql] IS NULL then 'N/A'  else [SourceUserDefinesql] end   as [Merge] ,
	[StorageContainter] ,
	concat(DestinationSchema,'_d') as FoundationSchema      ,
	[StorageFilePath] as FileDirectory ,
	--REPLACE([StorageFilePath],[StorageFilePath],'/mnt/migration/dev/data/raw/uk.sycle/') AS FileDirectory ,
	[StorageFileName] AS FileName ,
	[Active_Flag] ,
	[Archival_Flag] ,
	[GroupID] ,
	[OrderID] ,
	[SiteCode] ,
	--Case when [LoadType]='F' then 'Full' Else 'Delta' End as LoadType  
	Case when @LoadType='F' then 'Full' else 'Delta' end  as LoadType
	FROM  [audit_mig].[adf_datapipeline_config]  t   JOIN [audit_mig].[Batch] b 
	ON t.Source_Flag=b.Process_Name and b.Batch_Status='Running'  
	where t.Source_Flag='RAW-TO-FOUNDATION' AND   
	t.Active_Flag=1  and 
	SiteCode=@sitecode
END

ELSE IF @env='test' 

BEGIN
	SELECT  [ID],
	CAST(b.[Batch_ID] as varchar) as Batch_ID,
	cast(b.[Batch_Start_DateTime] as datetime) as Batch_Start_DateTime ,
	concat([SourceSchema],'_t') as [SourceSchema],
	concat([SourceSchema],'') as [p_SourceSchema],
	[SourceTable] , 
	--case when [LoadType]='F'  then  'N/A'  else [SourceUserDefinesql] end   as [Merge] ,
	case when [SourceUserDefinesql] IS NULL then 'N/A'  else [SourceUserDefinesql] end   as [Merge] ,
	[StorageContainter] ,
	concat(DestinationSchema,'_t') as FoundationSchema      ,
	[StorageFilePath] as FileDirectory ,
	--REPLACE([StorageFilePath],[StorageFilePath],'/mnt/migration/dev/data/raw/uk.sycle/') AS FileDirectory ,
	[StorageFileName] AS FileName ,
	[Active_Flag] ,
	[Archival_Flag] ,
	[GroupID] ,
	[OrderID] ,
	[SiteCode] ,
	--Case when [LoadType]='F' then 'Full' Else 'Delta' End as LoadType
	Case when @LoadType='F' then 'Full' else 'Delta' end  as LoadType
	FROM  [audit_mig].[adf_datapipeline_config]  t   JOIN [audit_mig].[Batch] b 
	ON t.Source_Flag=b.Process_Name and b.Batch_Status='Running'  
	where t.Source_Flag='RAW-TO-FOUNDATION' AND   
	t.Active_Flag=1  and 
	SiteCode=@sitecode
END
ELSE

BEGIN
		SELECT  [ID],
		CAST(b.[Batch_ID] as varchar) as Batch_ID,
		cast(b.[Batch_Start_DateTime] as datetime) as Batch_Start_DateTime ,
		[SourceSchema] , 
		concat([SourceSchema],'') as [p_SourceSchema],
		[SourceTable] , 
		case when [SourceUserDefinesql] IS NULL then 'N/A'  else [SourceUserDefinesql] end   as [Merge] ,
		[StorageContainter] ,
		DestinationSchema as FoundationSchema      ,
		--REPLACE([StorageFilePath],[StorageFilePath],'/mnt/migration/prod/data/raw/uk.sycle/') AS FileDirectory ,
		[StorageFilePath] as FileDirectory ,
		[StorageFileName] AS FileName ,
		[Active_Flag] ,
		[Archival_Flag] ,
		[GroupID] ,
		[OrderID] ,
		[SiteCode] ,
		--Case when [LoadType]='F' then 'Full' Else 'Delta' End as LoadType
		Case when @LoadType='F' then 'Full' else 'Delta' end  as LoadType
		FROM  [audit_mig].[adf_datapipeline_config]  t   JOIN [audit_mig].[Batch] b 
		ON t.Source_Flag=b.Process_Name and b.Batch_Status='Running'  
		where t.Source_Flag='RAW-TO-FOUNDATION' AND   
		t.Active_Flag=1  and 
		SiteCode=@sitecode
END

	Return;
	
END
GO


