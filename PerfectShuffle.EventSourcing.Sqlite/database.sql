CREATE TABLE [Commit] (
    CommitVersion   INTEGER  PRIMARY KEY ASC
                             NOT NULL,
    StreamName      TEXT     NOT NULL,
    StreamVersion   INTEGER  NOT NULL,
    DeduplicationId GUID     NOT NULL,
    EventType       TEXT     NOT NULL,
    Headers         BLOB,
    Payload         BLOB     NOT NULL,
    EventStamp      DATETIME NOT NULL,
    CommitStamp     DATETIME NOT NULL
);
