CREATE TYPE [dbo].[EventList] AS TABLE(		
	[SeqNumber] INT NOT NULL, -- Sequence number starting from zero
	[DeduplicationId] UNIQUEIDENTIFIER NOT NULL,
	[EventType] [varchar](5000) NOT NULL,
	[Headers] IMAGE NULL,
	[Payload] IMAGE NOT NULL,
	[EventStamp] [datetime2] NOT NULL
	PRIMARY KEY CLUSTERED ([DeduplicationId] ASC)   
	);

CREATE TABLE [dbo].[Commit](
	[CommitVersion] BIGINT IDENTITY(1,1) NOT NULL,	
	[StreamName] [varchar](40) NOT NULL,
	[StreamVersion] BIGINT NOT NULL,
	[DeduplicationId] UNIQUEIDENTIFIER NOT NULL,
	[EventType] [varchar](5000) NOT NULL,
	[Headers] IMAGE NULL,
	[Payload] IMAGE NOT NULL,
	[EventStamp] [datetime2] NOT NULL,
	[CommitStamp] [datetime2] NOT NULL DEFAULT SYSDATETIME(),
	CONSTRAINT [PK_Commit] PRIMARY KEY ([CommitVersion])
);

GO	

CREATE UNIQUE NONCLUSTERED INDEX IX_CommitId ON [dbo].[Commit] ([CommitVersion]);
GO

CREATE UNIQUE INDEX IX_Stream ON [dbo].[Commit] ([StreamName], [StreamVersion]);
GO

CREATE PROCEDURE [dbo].[usp_StoreEvents]
	@StreamName [varchar](40),	
	@EventList dbo.EventList ReadOnly,	
	@ExpectedStartVersion BIGINT = NULL,
	@EndVersion BIGINT OUTPUT
AS

BEGIN
	
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SET NOCOUNT ON;
	SET XACT_ABORT ON; -- Turns on rollback if T-SQL statement raises a run-time error.
      
  BEGIN TRANSACTION

	DECLARE @UniqueEventList dbo.EventList;

	DECLARE @DateCreated DATETIME = GetDate();

	DECLARE @StartIndex BIGINT

	SELECT @StartIndex = ISNULL(MAX([StreamVersion]), 0) FROM [Commit] WHERE [StreamName] = @StreamName		
	SELECT @EndVersion = @StartIndex + Count([SeqNumber]) FROM @EventList

	IF @ExpectedStartVersion IS NOT NULL AND @StartIndex + 1 <> @ExpectedStartVersion 		
	BEGIN
		THROW 53001, 'Unexpected start index', 1;
	END
	ELSE
		--Insert Event
		INSERT INTO [dbo].[Commit] ([StreamName], [StreamVersion], [DeduplicationId], [EventType], [Headers], [Payload], [EventStamp])
		SELECT @StreamName, eml.[SeqNumber] + @StartIndex + 1, eml.[DeduplicationId], eml.EventType, eml.Headers, eml.PayLoad, eml.EventStamp FROM @EventList eml
		LEFT JOIN [dbo].[Commit] em ON (em.[DeduplicationId] = eml.[DeduplicationId])
		WHERE em.[DeduplicationId] IS NULL
		ORDER BY eml.[SeqNumber] ASC

	COMMIT TRANSACTION

END
GO

CREATE PROCEDURE [dbo].[usp_GetStreamEvents]
	@StreamName Varchar(40),
	@FromStreamVersion Bigint,
	@BatchSize Int
AS
	select top(@BatchSize) 
		c.[CommitVersion],
		c.[StreamName],
		c.[StreamVersion],
		c.[DeduplicationId],
		c.[EventType],
		c.[Headers], 
		c.[Payload], 
		c.[EventStamp], 
		c.[CommitStamp] 
	from [dbo].[Commit] c 
	where 
	c.[StreamName] = @StreamName and 
	c.[StreamVersion] >= @FromStreamVersion
GO

CREATE PROCEDURE [dbo].[usp_GetEvents]	
	@FromCommitVersion Bigint,
	@BatchSize Int
AS
	select top(@BatchSize) 
		c.[CommitVersion],
		c.[StreamName],
		c.[StreamVersion],
		c.[DeduplicationId],
		c.[EventType],
		c.[Headers], 
		c.[Payload], 
		c.[EventStamp], 
		c.[CommitStamp] 
	from [dbo].[Commit] c 
	where 	
	c.[CommitVersion] >= @FromCommitVersion

GO